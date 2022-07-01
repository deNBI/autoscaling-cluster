## Autoscaling usage and settings 

### install
* select the ubuntu 20.04 cluster image in `simpleVM` 
* download `autoscaling.py` and `autoscaling_config.yaml` to `${HOME}/autoscaling/`
* define a new cluster password with `autoscaling -p`


### usage
* use `autoscaling -h` for options
```
-v          -version                        print the current version number
-h          -help                           print this help information
-fv         -flavors                        print available flavors
-m          -mode               mode        scaling with a high adaptation
-m          -mode               ...         select mode name from yaml file
-p          -password                       set cluster password
-rsc        -rescale                        run scaling with ansible playbook 
-s          -service                        run as service (mode: default)
-s          -service             mode       run as service
-sdc        -scaledownchoice                scale down (worker id) - interactive mode
-sdb        -scaledownbatch                 scale down (batch id)  - interactive mode
-suc        -scaleupchoice                  scale up - interactive mode
-sdi        -scaledownidle                  scale down all workers (idle + worker check)
-csd        -clustershutdown                delete all workers from cluster (api)
            -reset                          re-download autoscaling and reset configuration
_                                           no argument - mode: default (yaml config)
```
#### 

### start as systemd service
* `sudo service autoscaling start`
* `sudo service autoscaling stop`

### configuration
* config parameters are located at `${HOME}/autoscaling/autoscaling_config.yaml`
    * modes can be edited and defined
    * the default mode is defined as `default`
* parameters
  * limits
    * `limit_memory`: memory limit in TB
    * `limit_worker_starts`: worker start limit per flavor and cycle
    * `limit_workers`: limit the number of active workers
  * scaling values without job history
    * `scale_force`: initial scale force value, larger results in a higher maximum scale up value
    * `scale_frequency`: wait time in seconds before scale down idle workers without pending jobs
    * `worker_weight`: reduce starting new workers based on the number of current existing workers over the `scale_force`
  * scaling values for job history
    * `job_match_search`: True = active, search for similar jobs in history
    * `job_time_flavor`: if jobs are not in history, use the average job time from flavor data
    * `job_match_similar`: with a lower value (range 0-1), tend to start more new workers for jobs with a short runtime
    * job name modification:
      * `job_match_remove_numbers`: numbers not included in the comparison
      * `job_match_remove_num_brackets`: numbers in brackets not included in the comparison
      * `job_match_remove_pattern`: remove string from job names, ex 'wPipeline_'
      * `job_match_remove_text_within_parentheses`: remove any string within parentheses
  * flavor selection
    * `flavor_default`: if filled, use the default flavor - select the maximum required flavor!
    * `flavor_cut`: ex. 0.9 - cut 10% of the lower flavors, or remove a number of flavors
    * ephemeral flavors
      * `flavor_ephemeral`: only use ephemeral flavors by automatic scaling
      * `tmp_disk_check`: if active, jobs and worker need a tmp_disk value
      * possible combination:
        * `flavor_ephemeral: True` and `tmp_disk_check: False`
        * `flavor_ephemeral: False` and `tmp_disk_check: True`
  * `flavor_depth`
    * Pre-Launch the next x workers by flavor, only useful with a job priority with high resources first.
    * `0`: single flavor, start only workers for next jobs in queue
    * `-1`: start multiple flavors in one iteration, may break start-up limits
    * `-2`: no flavor data separation, select the highest flavor, similar flavor data not compatible
    * `-3`: single flavor in one iteration, but search all flavor levels, start the first (highest) flavor with generated scale up data
    * x (positive value): single flavor in one iteration, search for new workers by the next x flavors
      
