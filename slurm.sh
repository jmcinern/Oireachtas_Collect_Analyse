#!/bin/bash
#SBATCH --job-name=oireachtas_XML_collect
#SBATCH --output=./out/oireachtas_XML_collect%j.out
#SBATCH --error=./err/oireachtas_XML_collect_%j.err
#SBATCH --time=2:00:00
#SBATCH --partition=k2-medpri  # Changed from k2-gpu-v100
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --mem=16G  # Reduced from 256G
#SBATCH --mail-type=ALL
#SBATCH --mail-user=josephmcinerney7575@gmail.com

# Load only required modules (no GPU modules needed)
module load python3/3.10.5/gcc-9.3.0

# Activate your environment
source /mnt/scratch2/users/40460549/cpt-dail/myenv_new/bin/activate

pip instrall --no-cache-dir -r "requirements.txt"

# Navigate to project directory
cd $SLURM_SUBMIT_DIR

export HF_TOKEN=""

# Update synthesis.py to use 30,000 placenames
python Collect.py