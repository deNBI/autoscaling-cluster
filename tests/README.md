# Example scripts

All scripts can be executed either locally or using the SLURM scheduler by providing the corresponding profile:

## Prerequisites

Java must be installed on the muster node. On ubuntu it can be installed via apt (`sudo apt install default-jre`)

```
-profile PROFILE
```

where PROFILE can be either "slurm" or "standard".

## Parameter

`--memoryLimit` defines the memory size available for a process. (default: 60 GB)

`--numberOfTasks`  sets the number of tasks to execute. (default: 10)

