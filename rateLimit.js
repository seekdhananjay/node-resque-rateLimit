// Plugin to control the rate limit of jobs; for example only 100 jobs per 5 mins.

var rateLimit = function(worker, func, queue, job, args, options){
  var self = this;

  if(!options.jobsLimit){ options.jobsLimit = 0; }
  if(!options.timeLimit){ options.timeLimit = 0; }
  if(!options.limitCounterId){ options.limitCounterId = "limitCounterId"; }

  self.name = 'rateLimit';
  self.ttlPollingTime = 10*1000; // In MilliSeconds

  self.worker = worker;
  self.queue = queue;
  self.func = func;
  self.job = job;
  self.args = args;
  self.options = options;
  
  if(self.worker.queueObject){
    self.queueObject = self.worker.queueObject;
  }else{
    self.queueObject = self.worker;
  }
  
};
 
//////////////////// 
// PLUGIN METHODS // 
//////////////////// 

rateLimit.prototype.redis = function(){
  var self = this;
  return self.queueObject.connection.redis;
}; 

rateLimit.prototype.counterKey = function(counterId){
  var self = this;
  return self.queueObject.connection.key('counter:'+counterId).replace(/\s/, '');
};

rateLimit.prototype.interruptWorker = function(toggleWorkerRunningStatus){
  var self = this;
  console.log("Worker:", self.worker.name, " interrupted");
  self.worker.running = toggleWorkerRunningStatus;
  self.worker.pause();
};
rateLimit.prototype.after_perform = function(callback){
  var self = this;
  
  if(self.options.jobsLimit && self.options.timeLimit){
    var counterId, counterName, redis, expiryTimeInSeconds;
    
    counterId = self.options.limitCounterId;
    counterName = self.counterKey(counterId);
    redis = self.redis();
    
    redis.incr(counterName, function(error, jobCounter){
      if(error){ return callback(error); }
      if(jobCounter === 1){
        expiryTimeInSeconds = self.options.timeLimit/1000;
        redis.expire(counterName, expiryTimeInSeconds, function(error, result){
          if(error){ return callback(error); }
          callback(null, true);          
        });
      }else if(jobCounter == self.options.jobsLimit){
        // If Jobs Limit has been reached
        var getTTL = function(){
          redis.ttl(counterName, function(err, ttl){
            if(ttl<0){
              clearInterval(setIntervalId);
              // after the Time Limit, send interrupt to worker to resume
              self.interruptWorker(true);
            }else{
              console.log("Still need to wait for", ttl, "seconds.");
            }
          });
        };
        var setIntervalId = setInterval(getTTL, self.ttlPollingTime);
        callback(null, true);
        // send interrupt to worker to pause till the Time Limit
        self.interruptWorker(false);    
      }
    });
  }else{
    callback(null, true);
  }
}

module.exports = rateLimit;