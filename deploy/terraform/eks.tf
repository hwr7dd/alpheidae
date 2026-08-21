# EKS cluster with two node pools:
#   system      — blitz-meta StatefulSet (regular instances)
#   firecracker — privileged DaemonSet on bare metal (KVM)
module "eks" {
  count   = var.deployment_mode == "eks" ? 1 : 0
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = local.name
  cluster_version = "1.29"

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  enable_cluster_creator_admin_permissions = true

  eks_managed_node_groups = {
    system = {
      name           = "system"
      instance_types = [var.system_instance_type]
      min_size       = var.system_node_count
      max_size       = var.system_node_count
      desired_size   = var.system_node_count

      labels = {
        "blitz.io/tier" = "system"
      }
    }

    firecracker = {
      name           = "firecracker"
      instance_types = [var.firecracker_instance_type]
      min_size       = var.firecracker_node_count
      max_size       = var.firecracker_node_count
      desired_size   = var.firecracker_node_count
      ami_type       = "AL2_x86_64"

      labels = {
        "blitz.io/tier" = "firecracker"
      }

      taints = {
        firecracker = {
          key    = "blitz.io/firecracker"
          value  = "true"
          effect = "NO_SCHEDULE"
        }
      }

      # Bare metal needs larger root volume for snapshots
      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size = 500
            volume_type = "gp3"
            encrypted   = true
          }
        }
      }
    }
  }
}
