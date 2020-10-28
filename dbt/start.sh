#!/bin/bash

gcloud auth application-default login

export DBT_DATASET=$1

/bin/bash