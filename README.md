# autoscaling cluster

## required files

### cluster password

The cluster password should be in the same folder as the autoscaling program. File content must be a json object:
`{"password":"CLUSTER_PASSWORD"}`

## portal authentication file

This authentication file should be in the same folder as the autoscaling program. File content must be a json object:
`{"server":"portaldev","password":"PORTAL_PASSWORD"}`

## slurm configuration

### install requirements

```
sudo apt-get install -y mariadb-server slurmdbd python3-mysqldb
```

### slurm accounting storage and priority settings

Add the configuration to slurm.conf

* `/etc/slurm-llnl/slurm.conf`
* `/home/ubuntu/playbook/roles/common/templates/slurm/slurm.conf`

```
AccountingStorageType=accounting_storage/slurmdbd
AccountingStoreJobComment=YES

PriorityType=priority/multifactor
PriorityFlags=MAX_TRES
PriorityFavorSmall=NO # optional, reverse
PriorityWeightJobSize=100000
AccountingStorageTRES=cpu,mem,gres/gpu
PriorityWeightTRES=cpu=1000,mem=2000,gres/gpu=3000

TaskPlugin=task/cgroup
```

* view priority with: `squeue -o "%A %C %m %Q %N"`
* node info: `sinfo -o "%e %m %s %S"`

#### cgroup configuration

File `/etc/slurm-llnl/cgroup` should contain the following configuration:

```
AllowedDevicesFile="/etc/slurm-llnl/cgroup_allowed_devices_file.conf
CgroupReleaseAgentDir="/etc/slurm-llnl/cgroup
```

This folder should exist:
`sudo mkdir -v /etc/slurm-llnl/cgroup`

#### accounting storage configuration

`/etc/slurm-llnl/slurmdbd.conf`

```
AuthType=auth/munge
AuthInfo=/var/run/munge/munge.socket.2
DbdHost=localhost
DebugLevel=debug5
StorageHost=localhost
StorageLoc=slurm_acct_db
StoragePass=YOUR_DB_PASSWORD
StorageType=accounting_storage/mysql
StorageUser=slurm
LogFile=/var/log/slurm-llnl/slurmdbd.log
PidFile=/run/slurmdbd.pid
SlurmUser=slurm
```

#### install mysql database

```
sudo mysql -u $user -p"$passsword" -Bse "create user 'slurm'@'localhost' identified by 'YOUR_DB_PASSWORD';"
sudo mysql -u $user -p"$passsword" -Bse "grant all on slurm_acct_db.* TO 'slurm'@'localhost';"
SHOW DATABASES; # database should listed
sudo sacctmgr add cluster bibigrid
```

#### restart services

```
sudo systemctl restart slurmd.service
sudo systemctl restart slurmdbd.service
sudo systemctl restart slurmctld.service
```

##### possible problems

###### Error: `"Database is busy or waiting for lock from other user."`

```
sudo service mysql restart
sudo systemctl restart slurmdbd.service
```

###### Error: no port and ip registered

* check with `sacctmgr show cluster`
* possible fix `sudo systemctl restart slurmctld.service`

###### Error: cluster not available

* check with `sacctmgr show cluster`
* possible fix `sudo sacctmgr add cluster bibigrid`

###### Error: slurmdbd.service: Failed with result 'timeout'

* kill running slurmdbd services
* `sudo -H -u slurm bash -c slurmdbd -Dvvv`

### pyslurm

#### pyslurm @ ubuntu 18.04

##### requirements

```
apt install -y \
 git devscripts equivs apt-utils  libslurm-dev python3-setuptools cython python3-dev \
 libslurm-dev ca-certificates build-essential javascript-common python3-flask python3-ldap \
 libslurmdb-dev  # missing slurmdb.h ubuntu 18.04 only
```

##### location and slurm version

```
INSTALLPATH="/usr/src/"
SLURM_VER=17.11.0
```

#### pyslurm @ ubuntu 20.04

##### requirements

```
apt install -y \
 git devscripts equivs apt-utils  libslurm-dev \ 
 python3-setuptools cython3 python3-dev \
 libslurm-dev ca-certificates build-essential javascript-common \
 python3-flask python3-ldap python-is-python3
```

##### location and slurm version

```
INSTALLPATH="/usr/src/"
SLURM_VER=19.05.0
```

#### install

```
apt -y install libslurm-perl libslurmdb-perl slurm-wlm-basic-plugins-dev

cd ${INSTALLPATH} && \
 git clone https://github.com/PySlurm/pyslurm.git && \
 cd pyslurm && \
 git checkout remotes/origin/$SLURM_VER  && \
 sed -i 's/slurmfull/slurm/' setup.py  && \
 python setup.py build --slurm=/usr/ --slurm-inc=/usr/include/ --slurm-lib=/usr/lib/x86_64-linux-gnu/  && \
 python setup.py install && \
 python setup.py clean
```

### automatic start with crontab

For example, start the program every 10 minutes:

`0,10,20,30,40,50 *  * * *   ubuntu  python3 /home/ubuntu/autoscalingFolder/autoscaling.py`

### nextflow

#### install requirements

```
sudo apt-get install -y openjdk-17-jdk openjdk-17-demo \
 openjdk-17-doc openjdk-17-jre-headless \
 openjdk-17-source 
```

#### download

```
cd ~ 
mkdir -v ~/nextflow
cd ~/nextflow 
curl -s https://get.nextflow.io | bash

```

#### create alias

```
echo 'alias nextflow="${HOME}/nextflow/nextflow"'>>~/.bash_aliases
. ~/.bash_aliases
```
