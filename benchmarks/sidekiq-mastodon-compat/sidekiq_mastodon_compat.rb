# frozen_string_literal: true

# Sidekiq 8.1.7 run the way Mastodon runs it: the `sidekiq` command with Mastodon's own
# config/sidekiq.yml (v4.7.6: the queues default, push, ingress, mailers, pull, scheduler, fasp with
# their weights), at 25, 100, 500 and 1,000 threads. Mastodon's job classes need its database, so the
# jobs here are stand-ins that do what matters to the queue: each job is pushed to one of those queues
# (in Mastodon's proportions) and, when it runs, records its job id in a Redis set. A job that runs
# twice, or not at all, shows in that set.
#
# For every thread count: the jobs enqueued, then the server started; every job must run exactly once,
# every queue must end empty, nothing may fail or land in the retry or dead sets, and the server must
# stop on TERM. Beyond the threads: a scheduled job (`perform_in`), and a job that fails once and
# succeeds on its retry.
#
# Usage: ruby sidekiq_mastodon_compat.rb --config sidekiq.yml --junitxml out.xml
# REDIS_URL names the server (default redis://127.0.0.1:6379/0).

require "json"
require "optparse"
require "securerandom"
require "tmpdir"
require "sidekiq"
require "sidekiq/api"
require "cgi"

opts = { config: File.join(__dir__, "sidekiq.yml"), jobs: 10_000, threads: [25, 100, 500, 1000] }
OptionParser.new do |o|
  o.on("--config PATH") { |v| opts[:config] = v }
  o.on("--junitxml PATH") { |v| opts[:junitxml] = v }
  o.on("--jobs N", Integer) { |v| opts[:jobs] = v }
  o.on("--threads LIST") { |v| opts[:threads] = v.split(",").map(&:to_i) }
end.parse!

URL = ENV.fetch("REDIS_URL", "redis://127.0.0.1:6379/0")
Sidekiq.configure_client { |c| c.redis = { url: URL, size: 5 } }

# Jobs per queue, in the proportions of Mastodon's weights (default 8, push 6, ingress 4, mailers 2),
# and a few of the unweighted ones.
SPREAD = { "default" => 8, "push" => 6, "ingress" => 4, "mailers" => 2, "pull" => 1, "scheduler" => 1,
           "fasp" => 1 }.freeze

rows = []
t_start = Time.now

def check(rows, group, name)
  t = Time.now
  detail = nil
  begin
    detail = yield
  rescue StandardError => e
    detail = "#{e.class}: #{e.message}\n#{e.backtrace.first(3).join("\n")}"
  end
  rows << { group: group, name: name, detail: detail, ms: ((Time.now - t) * 1000).round }
  puts "#{detail ? 'FAIL' : 'PASS'}  #{group}: #{name}#{detail ? "  <- #{detail.lines.first.strip}" : ''}"
  $stdout.flush
end

def redis(&block)
  Sidekiq.redis(&block)
end

def wait_for(timeout)
  deadline = Time.now + timeout
  loop do
    v = yield
    return v if v
    return nil if Time.now > deadline

    sleep 0.2
  end
end

def start_server(opts, threads, log)
  env = { "REDIS_URL" => URL }
  Process.spawn(env, "bundle", "exec", "sidekiq", "-r", File.join(__dir__, "worker.rb"), "-C", opts[:config],
                "-c", threads.to_s, "-e", "production", %i[out err] => [log, "w"])
end

def stop_server(pid)
  Process.kill("TERM", pid)
  deadline = Time.now + 40
  loop do
    done, status = Process.wait2(pid, Process::WNOHANG)
    return status if done
    return nil if Time.now > deadline

    sleep 0.2
  end
end

opts[:threads].each do |threads|
  group = "Sidekiq #{threads} threads"
  n = opts[:jobs]
  redis { |c| c.call("FLUSHALL") }
  weights = SPREAD.flat_map { |q, w| [q] * w }
  by_queue = Hash.new(0)
  n.times do |i|
    q = weights[i % weights.size]
    by_queue[q] += 1
  end
  by_queue.each do |q, count|
    Sidekiq::Client.push_bulk("class" => "StandInJob", "queue" => q, "args" => Array.new(count) { |i| [q, i] })
  end
  server = nil
  log = File.join(Dir.tmpdir, "sidekiq-#{threads}.log")
  started = Time.now
  begin
    server = start_server(opts, threads, log)
    drained = wait_for(180) { redis { |c| c.call("SCARD", "seen") }.to_i >= n }
    elapsed = (Time.now - started).round(1)
    sleep 1 # the last jobs' acknowledgement
    check(rows, group, "every job ran (#{n} jobs in #{elapsed} s)") do
      seen = redis { |c| c.call("SCARD", "seen") }.to_i
      drained && seen == n ? nil : "#{seen} of #{n} jobs ran in #{elapsed} s; log tail:\n#{File.read(log).lines.last(5).join}"
    end
    check(rows, group, "no job ran twice") do
      dup = redis { |c| c.call("GET", "dup") }.to_i
      dup.zero? ? nil : "#{dup} jobs ran twice"
    end
    check(rows, group, "queues are empty") do
      left = Sidekiq::Queue.all.to_h { |q| [q.name, q.size] }.reject { |_, v| v.zero? }
      left.empty? ? nil : "jobs left in #{left}"
    end
    check(rows, group, "nothing failed, retried or died") do
      s = Sidekiq::Stats.new
      bad = { failed: s.failed, retry: s.retry_size, dead: s.dead_size, busy: s.workers_size }
                .reject { |_, v| v.to_i.zero? }
      bad.empty? ? nil : "stats: #{bad}"
    end
    check(rows, group, "processed counter equals the jobs run") do
      # The server adds its count to the total every few seconds (the heartbeat).
      wait_for(20) { Sidekiq::Stats.new.processed >= n }
      processed = Sidekiq::Stats.new.processed
      processed == n ? nil : "processed #{processed}, want #{n}"
    end
  ensure
    if server
      status = stop_server(server)
      check(rows, group, "stops on TERM") do
        if status.nil?
          Process.kill("KILL", server)
          Process.wait(server)
          "did not stop within 40 s"
        elsif !status.success? && status.exitstatus != 0
          "exit status #{status.exitstatus}"
        end
      end
    end
  end
end

# A scheduled job and a retry, on one server.
redis { |c| c.call("FLUSHALL") }
Sidekiq::Client.push("class" => "StandInJob", "queue" => "default", "args" => ["scheduled", 0],
                     "at" => Time.now.to_f + 2)
Sidekiq::Client.push("class" => "FlakyJob", "queue" => "default", "args" => ["flaky"], "retry" => 3)
server = start_server(opts, 25, File.join(Dir.tmpdir, "sidekiq-small.log"))
begin
  check(rows, "Sidekiq scheduling", "a job scheduled 2 s ahead runs") do
    ok = wait_for(60) { redis { |c| c.call("EXISTS", "scheduled-ran") }.to_i == 1 }
    ok && Sidekiq::ScheduledSet.new.size.zero? ? nil : "did not run: schedule set #{Sidekiq::ScheduledSet.new.size}"
  end
  check(rows, "Sidekiq scheduling", "a job that fails once succeeds on its retry") do
    ok = wait_for(90) { redis { |c| c.call("GET", "flaky-attempts") }.to_i >= 2 && Sidekiq::RetrySet.new.size.zero? }
    attempts = redis { |c| c.call("GET", "flaky-attempts") }.to_i
    ok && attempts == 2 ? nil : "attempts #{attempts}, retry set #{Sidekiq::RetrySet.new.size}"
  end
ensure
  stop_server(server)
end

if opts[:junitxml]
  failed = rows.count { |r| r[:detail] }
  esc = ->(s) { CGI.escapeHTML(s.to_s) }
  xml = +%(<?xml version="1.0" encoding="UTF-8"?>\n<testsuites>\n)
  xml << %(<testsuite name="sidekiq-mastodon-compat" tests="#{rows.size}" failures="#{failed}" errors="0" skipped="0" time="#{(Time.now - t_start).round(3)}">\n)
  rows.each do |r|
    name = r[:name].sub(/ \(.*\)\z/, "")
    xml << %(<testcase classname="#{esc.call("sidekiq-mastodon.#{r[:group]}")}" name="#{esc.call(name)}" time="#{r[:ms] / 1000.0}")
    if r[:detail]
      xml << %(><failure message="#{esc.call(r[:detail][0, 400])}">#{esc.call(r[:detail])}</failure></testcase>\n)
    else
      xml << "/>\n"
    end
  end
  xml << "</testsuite>\n</testsuites>\n"
  File.write(opts[:junitxml], xml)
end
puts "#{rows.size} checks, #{rows.count { |r| r[:detail] }} failed"
