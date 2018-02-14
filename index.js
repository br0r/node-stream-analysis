const {
  Transform,
  Readable
} = require('stream');
const os = require('os');
const split = require('split');
const GroupStream = require('./src/group');

class Operation extends Transform {
  constructor(f) {
    super({
      objectMode: true
    });
    this.f = f;
  }

  _transform(chunk, enc, next) {
    this.next = next;
    let v = this.f.call(this, chunk);
    if (typeof v !== 'undefined') {
      this.push(v);
      next();
    }
  }
}

class Stream extends Transform {
  constructor(inStream) {
    super({
      objectMode: true
    });
    this.prev = inStream.pipe(this);
  }

  filter(f) {
    this.prev = this.prev.pipe(new Operation(function(x) {
      if (f(x)) this.push(x);
      this.next();
    }));
    return this;
  }

  map(f) {
    this.prev = this.prev.pipe(new Operation(function(x) {
      this.push(f(x));
      this.next();
    }));
    return this;
  }

  split() {
    this.prev = this.prev.pipe(split(undefined, undefined, {
      trailing: false
    }));
    return this;
  }

  tap(f) {
    this.prev = this.prev.pipe(new Operation(function(x) {
      f(x);
      return x;
    }));
    return this;
  }

  fromJSON() {
    return this.map(JSON.parse);
  }

  toJSON() {
    this.prev = this.prev.pipe(new Operation(x => {
      return JSON.stringify(x) + os.EOL;
    }));
    return this;
  }

  consume(out) {
    if (typeof out === 'function') {
      this.prev.on('data', out);
    } else {
      this.prev.pipe(out);
    }
  }

  reduce(f) {
    this.prev = this.prev.pipe(new Operation(function(x) {
      f.call(this, x);
      this.next();
    }));
    return this;
  }

  throttle(time) {
    let latest = Date.now();
    this.prev = this.prev.pipe(new Operation(function(x) {
      if (Date.now() - latest > time) {
        latest = Date.now();
        this.push(x);
      }
      this.next();
    }));
    return this;
  }

  groupBy(keys, override = false) {
    return new GroupStream(this, keys, override);
  }

  // TODO Add support for forks
  depth(n) {
    this.prev = this.prev.pipe(new Operation(function(x) {
      let i = n;
      let o = x;
      let keys = [];
      while (i--) {
        const k = Object.keys(o)[0];
        keys.push(k);
        o = o[k];
      }
      this.push({
        data: o,
        keys
      });
      this.next();
    }));
    return this;
  }

  _transform(chunk, enc, next) {
    next(null, chunk);
  }
}

module.exports = Stream;
