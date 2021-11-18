#!/bin/bash

gnome-terminal -- dask-scheduler
gnome-terminal -- dask-worker --nprocs auto tcp://10.0.2.15:8786
echo 'tcp://10.0.2.15:8786'
