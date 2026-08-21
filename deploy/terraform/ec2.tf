# EC2 fallback: single bare-metal instance running meta + firecracker via Docker.
# Cheaper than EKS when you don't need K8s orchestration (~$73/mo saved on control plane).

data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-x86_64-gp2"]
  }
}

resource "aws_security_group" "firecracker" {
  count       = var.deployment_mode == "ec2" ? 1 : 0
  name        = "${local.name}-firecracker"
  description = "Blitz Firecracker host"
  vpc_id      = module.vpc.vpc_id

  dynamic "ingress" {
    for_each = length(var.allowed_cidr_blocks) > 0 ? [1] : []
    content {
      description = "Query API (authenticated)"
      from_port   = 8080
      to_port     = 8080
      protocol    = "tcp"
      cidr_blocks = var.allowed_cidr_blocks
    }
  }

  ingress {
    description = "Meta catalog (VPC only)"
    from_port   = 7401
    to_port     = 7401
    protocol    = "tcp"
    cidr_blocks = [module.vpc.vpc_cidr_block]
  }

  dynamic "ingress" {
    for_each = length(var.ssh_allowed_cidr_blocks) > 0 ? [1] : []
    content {
      description = "SSH"
      from_port   = 22
      to_port     = 22
      protocol    = "tcp"
      cidr_blocks = var.ssh_allowed_cidr_blocks
    }
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "firecracker" {
  count                       = var.deployment_mode == "ec2" ? 1 : 0
  ami                         = data.aws_ami.amazon_linux.id
  instance_type               = var.firecracker_instance_type
  subnet_id                   = module.vpc.public_subnets[0]
  vpc_security_group_ids      = [aws_security_group.firecracker[0].id]
  iam_instance_profile        = aws_iam_instance_profile.firecracker.name
  associate_public_ip_address = true
  key_name                    = var.ssh_key_name != "" ? var.ssh_key_name : null

  root_block_device {
    volume_size = 500
    volume_type = "gp3"
    encrypted   = true
  }

  user_data = templatefile("${path.module}/../scripts/ec2-user-data.sh", {
    artifacts_bucket = aws_s3_bucket.artifacts.id
    warehouse_bucket = aws_s3_bucket.warehouse.id
    aws_region       = var.aws_region
  })

  tags = {
    Name = "${local.name}-firecracker"
  }
}
