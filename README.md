## autoscaling cluster

### autoscaling install

#### required packages
##### ubuntu 20.04
```console
apt-get install -y mariadb-server slurmdbd cython3 libslurm-dev
```
##### ubuntu 18.04
```console
apt install -y mariadb-server slurmdbd cython3 libslurm-dev libslurmdb-dev
```

#### cluster password
The cluster password should be in the same folder as the autoscaling program.

`cluster_pw.json`
```console
{"password":"CLUSTER_PASSWORD"}`
```
#### start as service
##### crontab
For example, start the program every 5 minutes:
```console
*/5 *  * * *   ubuntu  python3 /home/ubuntu/autoscaling/autoscaling.py
```
##### systemd
`/etc/systemd/system/autoscaling.service`
```console
[Unit]
Description=Autoscaling Service

[Service]
User=ubuntu
PIDFile=/home/ubuntu/autoscaling/autoscaling.pid
ExecStart=/usr/bin/python3 /home/ubuntu/autoscaling/autoscaling.py -service
```

### slurm configuration

#### slurm accounting storage

Add the configuration to slurm.conf

* `/etc/slurm-llnl/slurm.conf`
* `${HOME}/playbook/roles/common/templates/slurm/slurm.conf`

```console
ClusterName=bibigrid

AccountingStorageType=accounting_storage/slurmdbd
AccountingStoreJobComment=YES

```
###### priority basic (default)
```console
PriorityType=priority/basic
```
###### priority sorted by resources
```console
PriorityType=priority/multifactor
PriorityFlags=MAX_TRES
PriorityWeightJobSize=100000
AccountingStorageTRES=cpu,mem,gres/gpu
PriorityFavorSmall=NO 
PriorityWeightTRES=cpu=1000,mem=2000,gres/gpu=3000
```

##### slurmdbd configuration 20.04

`/etc/slurm-llnl/slurmdbd.conf`

```console
AuthType=auth/munge
AuthInfo=/var/run/munge/munge.socket.2
DbdHost=localhost
DebugLevel=info
StorageHost=localhost
StorageLoc=slurm_acct_db
StoragePass=YOUR_DB_PASSWORD
StorageType=accounting_storage/mysql
StorageUser=slurm
LogFile=/var/log/slurm-llnl/slurmdbd.log
PidFile=/run/slurmdbd.pid
SlurmUser=slurm
```
##### slurmdbd configuration 18.04

```console
AuthType=auth/munge
AuthInfo=/var/run/munge/munge.socket.2
DbdHost=localhost
DebugLevel=3
StorageHost=localhost
StorageLoc=slurm_acct_db
StoragePass=YOUR_DB_PASSWORD
StorageType=accounting_storage/mysql
StorageUser=slurm
LogFile=/var/log/slurm-llnl/slurmdbd.log
PidFile=/var/run/slurm-llnl/slurmdbd.pid
SlurmUser=slurm
```

##### install mysql database

```console
sudo mysql -u $user -p"$passsword" -Bse "create user 'slurm'@'localhost' identified by 'YOUR_DB_PASSWORD';"
sudo mysql -u $user -p"$passsword" -Bse "grant all on slurm_acct_db.* TO 'slurm'@'localhost';"
```

##### restart services and add cluster

```console
sudo systemctl restart slurmd.service
sudo systemctl restart slurmdbd.service
sudo systemctl restart mysql  
sudo sacctmgr add cluster bibigrid
sudo systemctl restart slurmctld.service
```

### pyslurm

#### pyslurm @ ubuntu 20.04

##### location and slurm version

##### install

```console
INSTALLPATH="/usr/src/"
SLURM_VER=19.05.0

cd ${INSTALLPATH} && \
 git clone https://github.com/PySlurm/pyslurm.git && \
 cd pyslurm && \
 git checkout remotes/origin/$SLURM_VER  && \
 sed -i 's/slurmfull/slurm/' setup.py  && \
 python3 setup.py build --slurm=/usr/ --slurm-inc=/usr/include/ --slurm-lib=/usr/lib/x86_64-linux-gnu/  && \
 python3 setup.py install && \
 python3 setup.py clean
```

#### pyslurm @ ubuntu 18.04

##### location and slurm version

##### install

```console
INSTALLPATH="/usr/src/"
SLURM_VER=17.11.0

cd ${INSTALLPATH} && \
 git clone https://github.com/PySlurm/pyslurm.git && \
 cd pyslurm && \
 git checkout remotes/origin/$SLURM_VER  && \
 ln -s /usr/include/slurm-wlm /usr/include/slurm  && \
 python3 setup.py build --slurm=/usr/ --slurm-inc=/usr/include/ --slurm-lib=/usr/lib/x86_64-linux-gnu/  && \
 python3 setup.py install && \
 python3 setup.py clean
```
