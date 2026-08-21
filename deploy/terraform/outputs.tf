output "deployment_mode" {
  value = var.deployment_mode
}

output "artifacts_bucket" {
  value = aws_s3_bucket.artifacts.id
}

output "warehouse_bucket" {
  value = aws_s3_bucket.warehouse.id
}

output "vpc_id" {
  value = module.vpc.vpc_id
}

output "eks_cluster_name" {
  value = var.deployment_mode == "eks" ? module.eks[0].cluster_name : null
}

output "eks_cluster_endpoint" {
  value = var.deployment_mode == "eks" ? module.eks[0].cluster_endpoint : null
}

output "ec2_firecracker_public_ip" {
  value = var.deployment_mode == "ec2" ? aws_instance.firecracker[0].public_ip : null
}

output "query_endpoint_hint" {
  value = var.deployment_mode == "eks" ? "kubectl get svc -n blitz blitz-query (after applying k8s manifests)" : "http://${aws_instance.firecracker[0].public_ip}:8080/v1/query"
}

output "ecr_meta_url" {
  value = aws_ecr_repository.meta.repository_url
}

output "ecr_query_url" {
  value = aws_ecr_repository.query.repository_url
}

output "warehouse_uri" {
  value = "s3://${aws_s3_bucket.warehouse.id}/warehouse"
}
