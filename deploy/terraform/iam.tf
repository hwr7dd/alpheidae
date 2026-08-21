data "aws_iam_policy_document" "firecracker_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "firecracker" {
  name               = "${local.name}-firecracker"
  assume_role_policy = data.aws_iam_policy_document.firecracker_assume.json
}

data "aws_iam_policy_document" "firecracker_s3" {
  statement {
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.artifacts.arn, "${aws_s3_bucket.artifacts.arn}/*"]
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.warehouse.arn, "${aws_s3_bucket.warehouse.arn}/*"]
  }
}

resource "aws_iam_role_policy" "firecracker_s3" {
  name   = "s3-access"
  role   = aws_iam_role.firecracker.id
  policy = data.aws_iam_policy_document.firecracker_s3.json
}

resource "aws_iam_instance_profile" "firecracker" {
  name = "${local.name}-firecracker"
  role = aws_iam_role.firecracker.name
}
