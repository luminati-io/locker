(function(module) {
    var LockQueue = require("./LockQueue");

    function LockQueueManager() {
        this.queues = {};
    };

    LockQueueManager.prototype.free_if_needed = function(name) {
        if (!this.queues[name] || this.queues[name].length)
    	    return;
	delete this.queues[name];
    };

    LockQueueManager.prototype.getQueue = function(name) {
        if (!this.queues[name]) {
            this.queues[name] = new LockQueue(name);
        }

        // need to release some queues that is empty

        return this.queues[name];
    };

    module.exports = LockQueueManager;
})(module);
