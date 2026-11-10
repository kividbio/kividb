provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Name    = "kividb-r121-baremetal"
      Project = "kividb-r121-baremetal"
    }
  }
}

resource "aws_key_pair" "bench" {
  key_name   = "kividb-r121-baremetal"
  public_key = file("${path.module}/r121_key.pub")
  tags = { Name = "kividb-r121-baremetal" }
}

data "aws_vpc" "default" {
  default = true
}

locals {
  # Avoid nondeterministic default-VPC subnet order (e.g. us-east-1c) where c7gn.12xlarge may have no capacity.
  effective_az = coalesce(var.availability_zone, "${var.aws_region}a")
}

data "aws_subnets" "in_effective_az" {
  count = var.subnet_id == null ? 1 : 0

  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }

  filter {
    name   = "availability-zone"
    values = [local.effective_az]
  }
}

locals {
  subnet_id = var.subnet_id != null ? var.subnet_id : data.aws_subnets.in_effective_az[0].ids[0]
}

data "aws_subnet" "selected" {
  id = local.subnet_id
}

# Cluster placement groups are bound to a single AZ. The name must include that AZ so changing
# availability_zone / subnet creates a new group; otherwise the old group (e.g. locked to 1c) rejects other AZs.
locals {
  placement_group_name_suffix = replace(data.aws_subnet.selected.availability_zone, "-", "")
}

data "aws_ami" "ubuntu_jammy_arm64" {
  most_recent = true
  owners      = ["099720109477"]

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-arm64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  filter {
    name   = "architecture"
    values = ["arm64"]
  }
}

resource "aws_placement_group" "benchmark" {
  count    = var.use_placement_group ? 1 : 0
  name     = "${var.project_name}-cluster-${local.placement_group_name_suffix}"
  strategy = "cluster"
  tags     = { Name = "kividb-r121-baremetal" }
}

resource "aws_security_group" "benchmark" {
  name_prefix = "${var.project_name}-"
  description = "SSH + Redis ports for benchmark client/server"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_cidr]
  }

  ingress {
    description = "Redis / Kivi between benchmark hosts"
    from_port   = 6379
    to_port     = 6380
    protocol    = "tcp"
    self        = true
  }

  ingress {
    description = "ICMP ping (runbook latency check)"
    from_port   = -1
    to_port     = -1
    protocol    = "icmp"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    create_before_destroy = true
  }

  tags = {
    Name = "kividb-r121-baremetal"
  }
}

resource "aws_instance" "server" {
  ami                    = data.aws_ami.ubuntu_jammy_arm64.id
  instance_type          = var.server_instance_type
  key_name               = aws_key_pair.bench.key_name
  subnet_id              = local.subnet_id
  vpc_security_group_ids = [aws_security_group.benchmark.id]

  placement_group = var.use_placement_group ? aws_placement_group.benchmark[0].name : null

  user_data = base64encode(file("${path.module}/user_data/server.sh"))

  root_block_device {
    volume_size           = var.server_root_volume_gb
    volume_type           = "gp3"
    delete_on_termination = true
  }

  # Large metal / high-vCPU instances can sit in "pending" for many minutes; do not interrupt apply.
  timeouts {
    create = "60m"
    delete = "30m"
  }

  tags = {
    Name = "kividb-r121-baremetal"
    Role = "redis-dragonfly-kividb-server"
  }
}

resource "aws_instance" "client" {
  ami                    = data.aws_ami.ubuntu_jammy_arm64.id
  instance_type          = var.client_instance_type
  key_name               = aws_key_pair.bench.key_name
  subnet_id              = local.subnet_id
  vpc_security_group_ids = [aws_security_group.benchmark.id]

  placement_group = var.use_placement_group ? aws_placement_group.benchmark[0].name : null

  user_data = base64encode(file("${path.module}/user_data/client.sh"))

  root_block_device {
    volume_size           = var.client_root_volume_gb
    volume_type           = "gp3"
    delete_on_termination = true
  }

  timeouts {
    create = "60m"
    delete = "30m"
  }

  tags = {
    Name = "kividb-r121-baremetal"
    Role = "memtier-client"
  }
}
