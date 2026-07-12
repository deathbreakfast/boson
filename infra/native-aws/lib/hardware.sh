#!/usr/bin/env bash
# Map EC2 instance type to boson-bench hardware profile tag.
# Usage: boson_hardware_tag_from_instance_type c6i.large  -> aws-c6i-large
boson_hardware_tag_from_instance_type() {
  local itype="${1:?instance type}"
  local tag="${itype//./-}"
  echo "aws-${tag}"
}
