# Job Definition
resource "aws_batch_job_definition" "generate_batch_jd_output" {
  name = "${var.prefix}-output"
  type = "container"
  platform_capabilities = ["FARGATE"]
  propagate_tags = true
  tags = { "job_definition": "${var.prefix}-output" }

  container_properties = jsonencode({
    image = "${local.account_id}.dkr.ecr.us-west-2.amazonaws.com/${var.prefix}-output:${var.image_tag}"
    executionRoleArn = var.iam_execution_role_arn
    jobRoleArn = var.iam_job_role_arn
    fargatePlatformConfiguration = {
      platformVersion = "LATEST"
    }
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group = aws_cloudwatch_log_group.cw_log_group.name
      }
    }
    resourceRequirements = [{
      type = "MEMORY"
      value = "65536"
    }, {
      type = "VCPU",
      value = "16"
    }]
    mountPoints = [{
      sourceVolume = "input",
      containerPath = "/mnt/data/input"
      readOnly = true
    }, {
      sourceVolume = "flpe"
      containerPath = "/mnt/data/flpe"
      readOnly = true
    }, {
      sourceVolume = "moi"
      containerPath = "/mnt/data/moi"
      readOnly = true
    }, {
      sourceVolume = "diagnostics"
      containerPath = "/mnt/data/diagnostics"
      readOnly = true
    }, {
      sourceVolume = "offline"
      containerPath = "/mnt/data/offline"
      readOnly = true
    }, {
      sourceVolume = "validation"
      containerPath = "/mnt/data/validation"
      readOnly = true
    }, {
      sourceVolume = "output"
      containerPath = "/mnt/data/output"
      readOnly = false
    }]
    volumes = [{
      name = "input"
      efsVolumeConfiguration = {
        fileSystemId = var.efs_file_system_ids["input"]
        rootDirectory = "/"
      }
    }, {
      name = "flpe"
      efsVolumeConfiguration = {
        fileSystemId = var.efs_file_system_ids["flpe"]
        rootDirectory = "/"
      }
    }, {
      name = "moi"
      efsVolumeConfiguration = {
        fileSystemId = var.efs_file_system_ids["moi"]
        rootDirectory = "/"
      }
    }, {
      name = "diagnostics"
      efsVolumeConfiguration = {
        fileSystemId = var.efs_file_system_ids["diagnostics"]
        rootDirectory = "/"
      }
    }, {
      name = "offline"
      efsVolumeConfiguration = {
        fileSystemId = var.efs_file_system_ids["offline"]
        rootDirectory = "/"
      }
    }, {
      name = "validation"
      efsVolumeConfiguration = {
        fileSystemId = var.efs_file_system_ids["validation"]
        rootDirectory = "/"
      }
    }, {
      name = "output"
      efsVolumeConfiguration = {
        fileSystemId = var.efs_file_system_ids["validation"]
        rootDirectory = "/"
      }
    }]
  })
}

# Log group
resource "aws_cloudwatch_log_group" "cw_log_group" {
  name = "/aws/batch/job/${var.prefix}-output/"
}
