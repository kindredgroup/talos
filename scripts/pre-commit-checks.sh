#!/bin/bash
set -e

make withenv RECIPE=lint
make withenv RECIPE=test.unit


