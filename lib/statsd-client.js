/*
 * Copyright © 2012, Morten Siebuhr <sbhr@sbhr.dk>
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
 * REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
 * INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
 * LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
 * OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
 * PERFORMANCE OF THIS SOFTWARE.
 *
 */

// From https://github.com/msiebuhr/node-statsd-client/blob/master/lib/statsd-client.js
// Adapted to behave similarly to pinstatsd (https://phabricator.pinadmin.com/diffusion/PS/)

var os = require("os");
var fs = require("fs");

var invalidCharacters = [':', '\0', '/', '\\'];

var maxFileLength = 251;

var canarySampleRate = 0.1;

function isCanary(){
    return fs.existsSync("/var/run/canary");
}

function isControl(){
    return fs.existsSync("/var/run/control");
}

function getHostType(){
    if (isCanary()){
        return "canary";
    }
    if (isControl()){
        return "control";
    }
    return "";
}

function initializeSocket(options){
    if(options.tcp) {
        //User specifically wants a tcp socket
        return new (require('./TCPSocket'))(options);
    } else if (options.host && options.host.match(/^http(s?):\/\//i)) {
        // Starts with 'http://', then create a HTTP socket
        return new (require('./HttpSocket'))(options);
    } else {
        // Fall back to a UDP ephemeral socket
        return new (require('./EphemeralSocket'))(options);
    }

}

/*
 * Set up the statsd-client.
 *
 * Requires the `hostname`. Options currently allows for `port`, `debug`, `defaultAddHostname`,
 * and `useOpentstbAlways` to be set.
 *
 */
function StatsDClient(options) {
    this.options = options || {};
    this.hostname = os.hostname();
    this.deployType = getHostType();
    this._helpers = undefined;

    // Set defaults
    this.options.prefix = this.options.prefix || "";
    this.options.instances = this.options.instances || null;
    if (this.options.defaultAddHostname === undefined){
      this.options.defaultAddHostname = true;
    }
    this.options.useOpentsdbAlways = this.options.useOpentsdbAlways || false;

    // Prefix?
    if (this.options.prefix && this.options.prefix !== "") {
        // Add trailing dot if it's missing
        var p = this.options.prefix;
        this.options.prefix = p[p.length - 1] === '.' ? p : p + ".";
    }

    this.initializeSockets(options);
}



/*
 * Initialize sockets for sending data to servers
 *
 */
StatsDClient.prototype.initializeSockets = function (options){
    options = options || {};
    if (options.instance !== undefined){
        if (options.instance.match(/^http(s?):\/\//i)){
          var split_instance = options.instance.split(":");
          if (split_instance.length === 2){
              options['host'] = options.instance;
              options['port'] = "80";
          } else if (split_instance.length === 3) {
              options['host'] = split_instance[0] + ":" + split_instance[1];
              options['port'] = split_instance[2];
          }
        } else {
          options['host'] = options.instance[0];
          options['port'] = options.instance[1] || "18125";
        }
        this.instanceSocket = initializeSocket(options);
    }
    // Copied from pinstatsd/client.py:23
    options['host'] = "127.0.0.1";
    options['port'] = "18127";
    this.localOpentsdbStatsdInstanceSocket = initializeSocket(options);
    options['port'] = "18125";
    this.localCarbonStatsdInstanceSocket = initializeSocket(options);
};

/*
 * Get the host:port from the pool of available instances that is a match for this metric
 *
 */
StatsDClient.prototype.getSocketForData = function (stat, tags){
    if (this.instanceSocket){
        return this.instanceSocket;
    }
    if (this.deployType || tags !== {} || this.useOpentsdbAlways){
        return this.localOpentsdbStatsdInstanceSocket;
    }
    return this.localCarbonStatsdInstanceSocket;
};

/*
 * Takes a tags object and puts it into the stat name so that statsd
 * can understand it.  The maximum number of tags is 8.  If addHostname
 * is True, then the host tag will be added.  If addHostname is falsey, then defaultAddHostname will
 * determine if a host tag is added.
 *
 */
StatsDClient.prototype.addTagsToStat = function (stat, tags, addHostname){
    if (!tags) {
        tags = {};
    }
    if (addHostname || this.options.defaultAddHostname){
      tags['host'] = this.hostname;
    }
    if (this.deployType) {
      tags['deployType'] = this.deployType;
    }
    Object.keys(tags).forEach(function(key){
        stat = stat + "._t_" + key + "." + tags[key];
        });
    return stat;
};

/*
 * gauge(name, value)
 */
StatsDClient.prototype.gauge = function (name, value, options) {
  var data = {};
  data[this.options.prefix + name] = value + "|g";
  this.send(data, options);
};

StatsDClient.prototype.gaugeDelta = function (name, delta, options) {
  var sign = delta >= 0 ? "+" : "-";
  var data = {};
  data[this.options.prefix + name] = sign + Math.abs(delta) + "|g";
  this.send(data, options);
};

/*
 * set(name, value)
 */
StatsDClient.prototype.set = function (name, value, options) {
  var data = {};
  data[this.options.prefix + name] = value + "|s";
  this.send(data, options);
};

/*
 * counter(name, delta)
 */
StatsDClient.prototype.counter = function (name, delta, options) {
  var data = {};
  data[this.options.prefix + name] = delta + "|c";
  this.send(data, options);
};

/*
 * increment(name)
 */
StatsDClient.prototype.increment = function (name, options) {
  options = options || {};
  var delta = options.delta || 1;
  this.counter(name, Math.abs(delta), options);
};

/*
 * decrement(name)
 */
StatsDClient.prototype.decrement = function (name, options) {
  options = options || {};
  var delta = options.delta || 1;
  this.counter(name, -1 * Math.abs(delta), options);
};

/*
 * timings(name, date-object | ms)
 */
StatsDClient.prototype.timing = function (name, time, options) {
  // Date-object or integer?
  var t = time instanceof Date ? new Date() - time : time;
  var data = {};
  data[this.options.prefix + name] = t + "|ms";
  this.send(data, options);
};

/*/
 * histogram(name, value)
 */
StatsDClient.prototype.histogram = function (name, value, options) {
  var data = {};
  data[this.options.prefix + name] = value + "|h";
  this.send(data, options);
};

/*
 * Close the socket, if in use and cancel the interval-check, if running.
 */
StatsDClient.prototype.close = function () {
  this._socket.close();
};

/*
 * Close the socket, if in use and cancel the interval-check, if running.
 */
StatsDClient.prototype.send = function (data, options) {
  options = options || {};

  var sampleRate = options['sampleRate'] || 1.0;
  var tags = options['tags'] || {};
  var addHostname = options['addHostname'] || false;

    var sampledData = {};
    if (this.deployType){
        if (canarySampleRate > sampleRate){
            sampleRate = canarySampleRate;
        }
    }

    if (sampleRate < 1.0 && Math.random() < sampleRate){
        Object.keys(data).forEach(function(key) {
            sampledData[key] = data[key] + "|@" + sampleRate;
        });
    } else {
        sampledData = data;
    }
    try {
        Object.keys(sampledData).forEach(function(stat){
            var key = stat;
            for (var i = 0; i < stat.len; i++){
                if (invalidCharacters.indexOf(stat[i]) !== -1){
                    //TODO add reasonable logging
                    console.log("Invalid character");
                    return;
                }
            }
            if (stat.length > maxFileLength){
                //TODO add reasonable logging
                console.log("Stat is too long to be recorded");
                return;
            }
            if (tags !== {} || this.deployType || this.useOpentsdbAlways){
                stat = this.addTagsToStat(stat, tags, addHostname);
            }
            var socket = this.getSocketForData(tags);
            var sendData = stat + ":" + sampledData[key];
            socket.send(sendData);
        }.bind(this));
    } catch (err) {
        // TODO(michaelp) better logging
        console.log(err);
        //pass
    } 

    
};

module.exports = StatsDClient;
