(function(module) {
    var net              = require("net"),
        Lock             = require("./lib/Lock"),
        LockQueueManager = require("./lib/LockQueueManager"),
        LockAction       = require("./lib/LockAction");

    function releaseLocks(locksRegistry) {
        Object.keys(locksRegistry).forEach(function(key) {
            locksRegistry[key].lock.release();
        });
    };
    function connName(conn)
    {
	return conn.remoteAddress+':'+conn.remotePort;
    }

    function Locker() {
        var manager = new LockQueueManager();
	this.conns = {};
	var _this = this;

        this.server = net.createServer(function(connection) {

            var locksRegistry   = {},
		conn_name = connName(connection),
                currentSequence = 0,
                data            = new Buffer(0),
                temp            = new Buffer(0);
	    _this.conns[conn_name] = locksRegistry;
	    var closing = false;

            connection.on("error", function(error) {
                connection.end();
		closing = true;
                releaseLocks(locksRegistry);
		delete _this.conns[conn_name];
            });

            connection.on("end", function() {
		closing = true;
                releaseLocks(locksRegistry);
		delete _this.conns[conn_name];
            });

            connection.on("timeout", function() {
		closing = true;
                releaseLocks(locksRegistry);
		delete _this.conns[conn_name];
            });

            connection.on("data", function(part) {
                var sequence,
                    wait,
                    timeout,
                    action,
                    length,
                    name;

                temp = new Buffer(data.length + part.length);
                data.copy(temp, 0);
                part.copy(temp, data.length);

                data = new Buffer(temp);

                while (data.length && ((data[0] + 14) <= data.length)) {
                    length   = data[0];
                    sequence = data.readUInt32LE(1);
                    wait     = data.readUInt32LE(5);
                    timeout  = data.readUInt32LE(9);
                    action   = data[13];
                    name     = data.slice(14, length + 14).toString();

                    currentSequence = sequence;

                    if (action == LockAction.ACTION_LOCK) {
                        lock(name, sequence, wait, timeout);
                    } else if (action == LockAction.ACTION_UNLOCK) {
                        unlock(sequence);
                    }

                    data = data.slice(length + 14);
                }
            });

            function lock(name, sequence, wait, timeout) {
                var lock = new Lock(name, manager);

                locksRegistry[sequence] = {lock: lock, request: Date.now()};

                lock.acquire(wait, timeout, function(error) {
		    if (error)
			locksRegistry[sequence].error = error;
		    else
			locksRegistry[sequence].acquired = Date.now();
                    respond(sequence, LockAction.ACTION_LOCK, error ? 0 : 1);
                });
            };

            function unlock(sequence) {
                var lock = locksRegistry[sequence];

                if (lock) {
                    delete locksRegistry[sequence];
                }

                respond(sequence, LockAction.ACTION_UNLOCK, lock &&
		    lock.lock.release());
            };

            function respond(sequence, action, result) {
		if (closing)
		    return; /* socket already closed */
                var response = new Buffer(6);

                response.writeUInt32LE(sequence, 0);
                response[4] = action;
                response[5] = result ? 1 : 0;

                connection.write(response, "binary");
            };
        });
    };

    Locker.prototype.listen = function() {
        this.server.listen.apply(this.server, arguments);
    };

    Locker.prototype.lockStatus = function() {
	var now = new Date();
	var stat = {};
	var _this = this;
	Object.keys(this.conns).forEach(function(c){
	    var registry = _this.conns[c];
	    stat[c] = Object.keys(registry).map(function(key){
		var _lock = registry[key];
		var res = {lock: _lock.lock.getName(),
		    request: now - _lock.request};
		if (_lock.acquired)
		    res.acquired = now - _lock.acquired;
		if (_lock.error)
		    res.error = _lock.error;
		return res;
	    });
	});
	return stat;
    };

    Locker.prototype.close = function() {
        this.server.close.apply(this.server, arguments);
    };

    module.exports = Locker;
})(module);
