variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "project_name" {
  type    = string
  default = "blitz"
}

# "eks" = EKS cluster with bare-metal Firecracker node pool (preferred)
# "ec2" = single bare-metal EC2 instance (simpler, no K8s control-plane cost)
variable "deployment_mode" {
  type        = string
  default     = "eks"
  description = "eks (Firecracker on EKS bare-metal DaemonSet) or ec2 (all-in-one bare metal)"
  validation {
    condition     = contains(["eks", "ec2"], var.deployment_mode)
    error_message = "deployment_mode must be eks or ec2"
  }
}

variable "firecracker_instance_type" {
  type        = string
  default     = "m5.metal"
  description = "Bare-metal instance with KVM. Options: m5.metal, i3.metal, c5.metal"
}

variable "system_instance_type" {
  type    = string
  default = "t3.medium"
}

variable "system_node_count" {
  type    = number
  default = 2
}

variable "firecracker_node_count" {
  type        = number
  default     = 1
  description = "Bare-metal nodes for Firecracker (EKS mode only)"
}

variable "allowed_cidr_blocks" {
  type        = list(string)
  default     = []
  description = "CIDRs allowed to reach the query API (8080). Empty = deny public; use VPN/bastion."
}

variable "ssh_allowed_cidr_blocks" {
  type        = list(string)
  default     = []
  description = "CIDRs allowed SSH access. Empty disables SSH ingress."
}
