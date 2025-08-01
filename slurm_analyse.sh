#!/bin/bash
#SBATCH --job-name=analyse
#SBATCH --output=./out/analyse_%j.out
#SBATCH --error=./err/analyse_%j.err
#SBATCH --time=00:30:00
#SBATCH --partition=k2-lowpri
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --mail-type=ALL
#SBATCH --mail-user=josephmcinerney7575@gmail.com

module load python3/3.10.5/gcc-9.3.0

source /mnt/scratch2/users/40460549/cpt-dail/myenv_new/bin/activate

cd $SLURM_SUBMIT_DIR

python Analyse.py