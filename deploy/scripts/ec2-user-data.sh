#!/bin/bash
# EC2 user-data: install Docker, pull/run blitz containers on bare metal.
set -euo pipefail
exec > /var/log/blitz-init.log 2>&1

ARTIFACTS_BUCKET="${artifacts_bucket}"
WAREHOUSE_BUCKET="${warehouse_bucket}"
AWS_REGION="${aws_region}"

yum update -y
yum install -y docker awscli
systemctl enable docker
systemctl start docker

mkdir -p /var/lib/blitz/snapshots

# Sync snapshots from S3 (upload first via deploy/scripts/build-artifacts.sh)
aws s3 sync "s3://${ARTIFACTS_BUCKET}/snapshots/" /var/lib/blitz/snapshots/ --region "$AWS_REGION" || true

# Meta catalog (single-node for EC2 dev; use EKS for HA)
docker run -d --name blitz-meta --restart always \
  -p 7401:7401 \
  -e BLITZ_META_ADDR=0.0.0.0:7401 \
  -e BLITZ_META_PEERS= \
  blitz-meta:latest || echo "Pull blitz-meta:latest first"

# Firecracker query host (privileged, needs /dev/kvm)
docker run -d --name blitz-firecracker --restart always \
  --privileged \
  --network host \
  -v /dev/kvm:/dev/kvm \
  -v /var/lib/blitz/snapshots:/var/lib/blitz/snapshots \
  -e BLITZ_ARTIFACTS_BUCKET="$ARTIFACTS_BUCKET" \
  -e BLITZ_WAREHOUSE_BUCKET="$WAREHOUSE_BUCKET" \
  blitz-firecracker-host:latest || echo "Pull blitz-firecracker-host:latest first"

echo "Blitz EC2 bootstrap complete"
