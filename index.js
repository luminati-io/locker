(function(module) {
    var net              = require("net"),
        fs               = require("fs"),
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

    function Locker(opt) {
        var manager = new LockQueueManager();
	this.conns = {};
        this.opt = opt||{};
	var _this = this;

        this.server = net.createServer(function(connection) {

            var locksRegistry   = {},
		conn_name = connName(connection),
                data            = new Buffer(0),
                temp            = new Buffer(0);
	    _this.conns[conn_name] = {
                locksRegistry,
                ip: connection.remoteAddress,
                pid: 0,
            }
	    var closing = false;

            connection.on("error", function(error) {
                connection.end();
		closing = true;
                releaseLocks(locksRegistry);
		delete _this.conns[conn_name];
                _this.save();
            });

            connection.on("end", function() {
		closing = true;
                releaseLocks(locksRegistry);
		delete _this.conns[conn_name];
                _this.save();
            });

            connection.on("timeout", function() {
		closing = true;
                releaseLocks(locksRegistry);
		delete _this.conns[conn_name];
                _this.save();
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

                    switch (action) {
                    case LockAction.ACTION_INIT:
                        _this.conns[conn_name].pid = sequence;
                        break;
                    case LockAction.ACTION_CONT:
                        _this.conns[conn_name].pid = sequence;
                        var db = _this.load(connection.remoteAddress, sequence);
                        if (db)
                        {
                            let acquired = db.filter(l=>!!l.acquired);
                            let waiting = db.filter(l=>!l.acquired);
                            for (let l of acquired)
                                lock(l.name, l.sequence, l.wait, l.timeout, {skip_respond: true});
                            for (let l of waiting)
                                lock(l.name, l.sequence, l.wait, l.timeout);
                        }
                        break;
                    case LockAction.ACTION_LOCK:
                        lock(name, sequence, wait, timeout);
                        break;
                    case LockAction.ACTION_UNLOCK:
                        unlock(sequence);
                        break;
                    }
                    _this.save();

                    data = data.slice(length + 14);
                }
            });

            function lock(name, sequence, wait, timeout, opt) {
                var lock = new Lock(name, manager);
                var opt = opt||{};

                locksRegistry[sequence] = {
                    lock: lock,
                    name: name,
                    sequence: sequence,
                    wait: wait,
                    timeout: timeout,
                    request: Date.now(),
                };

                lock.acquire(wait, timeout, function(error) {
		    if (error)
			locksRegistry[sequence].error = error;
		    else
			locksRegistry[sequence].acquired = Date.now();
                    if (!opt.skip_respond)
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
	    var registry = _this.conns[c].locksRegistry;
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

    Locker.prototype.load = function(ip, pid) {
        if (!this.opt.dbfile || !pid)
            return;
        var db = JSON.parse(fs.readFileSync(this.opt.dbfile).toString());
        var locks = db[ip+'/'+pid];
        if (!locks)
            return;
        var now = Date.now();
        return locks.map(l=>{
            l.wait = (l.request+l.wait)-now;
            l.timeout = (l.request+l.timeout)-now;
            if (l.wait<0)
                l.wait = 0;
            if (l.timeout<0)
                l.timeout = 0;
            return l;
        });
    }

    Locker.prototype.save = function() {
        if (!this.opt.dbfile)
            return;
        var db = {};
	var _this = this;
        Object.keys(this.conns).forEach(function(c){
            var key = _this.conns[c].ip+'/'+_this.conns[c].pid;
	    var registry = _this.conns[c].locksRegistry;
            db[key] = Object.keys(registry).map(k=>{
                var _lock = registry[k];
                var res = {
                    name: _lock.name,
                    sequence: _lock.sequence,
                    wait: _lock.wait,
                    timeout: _lock.timeout,
                    request: _lock.request,
                    acquired: _lock.acquired,
                };
                return res;
            });
        });
        console.log('Saving:\n'+JSON.stringify(db)+'\n\n');
        fs.writeFileSync(this.opt.dbfile, JSON.stringify(db));
    };

    module.exports = Locker;
})(module);
