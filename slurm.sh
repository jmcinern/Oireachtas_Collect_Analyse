#!/bin/bash
#SBATCH --job-name=fasttext
#SBATCH --output=./out/fasttext%j.out
#SBATCH --error=./err/fasttext_%j.err
#SBATCH --time=00:05:00
#SBATCH --partition=k2-lowpri  # Changed from k2-gpu-v100
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=16G  # Reduced from 256G
#SBATCH --mail-type=ALL
#SBATCH --mail-user=josephmcinerney7575@gmail.com

# Load only required modules (no GPU modules needed)
module load python3/3.10.5/gcc-9.3.0

# Activate your environment
source /mnt/scratch2/users/40460549/cpt-dail/myenv_new/bin/activate

# Navigate to project directory
cd $SLURM_SUBMIT_DIR

# Update synthesis.py to use 30,000 placenames
python Collect.py