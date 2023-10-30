# Example scripts

All scripts can be executed either locally or using the SLURM scheduler by providing the corresponding profile.

## Prerequisites

Java must be installed on the master node. On ubuntu it can be installed via apt (`sudo apt install default-jre`)
In addition you need to install Nextflow (`curl -s https://get.nextflow.io | bash`).

```
nextflow -profile PROFILE
```

where PROFILE can be either "slurm" or "standard".

## Additional Parameters

`--memoryLimit` defines the memory size available for a process. (default: 60 GB)

`--numberOfTasks`  sets the number of tasks to execute. (default: 10)

