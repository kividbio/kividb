# frozen_string_literal: true

# What the Sidekiq server loads (`sidekiq -r worker.rb`): the stand-in jobs of
# sidekiq_mastodon_compat.rb.
require "sidekiq"

Sidekiq.configure_server do |config|
  config.redis = { url: ENV.fetch("REDIS_URL", "redis://127.0.0.1:6379/0") }
end

# Records its job id: a job that runs twice raises `dup`; one that never runs is missing from `seen`.
class StandInJob
  include Sidekiq::Job

  def perform(tag, _index)
    Sidekiq.redis do |c|
      added = c.call("SADD", "seen", jid)
      c.call("INCR", "dup") if added.zero?
      c.call("SET", "scheduled-ran", "1") if tag == "scheduled"
    end
  end
end

# Fails on its first run, succeeds on the retry a second later.
class FlakyJob
  include Sidekiq::Job
  sidekiq_retry_in { |_count, _exception, _jobhash| 1 }

  def perform(_tag)
    attempts = Sidekiq.redis { |c| c.call("INCR", "flaky-attempts") }
    raise "first attempt fails" if attempts == 1
  end
end
