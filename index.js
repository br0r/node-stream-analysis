const {
  Transform,
  Readable
} = require('stream');
const os = require('os');
const split = require('split');

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
    this.prev = this.prev.pipe(new Operation(f));
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
    this.prev.pipe(out);
  }

  reduce(f) {
    this.prev = this.prev.pipe(new Operation(function(x) {
      f.call(this, x);
      this.next();
    }));
    return this;
  }

  throttle(time) {
    this.prev = this.prev.pipe(new Operation(function(x) {
      setTimeout(() => {
        this.push(x);
        this.next();
      }, time);
      return undefined;
    }));
    return this;
  }

  groupBy(keys, override = false) {
    const group = {};
    this.prev = this.prev.pipe(new Transform({
      objectMode: true,
      transform: function(chunk, enc, next) {
        let o = group;
        let prevX;
        for (let i = 0; i < keys.length; i += 1) {
          let k = keys[i];
          if (!chunk[k]) {
            next();
            break;
          }
          let x = chunk[k];
          if (i + 1 < keys.length) {
            if (!o[x]) {
              o[x] = {};
            }
            o = o[x];
          } else {
            if (!o[x]) {
              if (!override) o[x] = [];
            }
          }
          prevX = x;
        }
        if (override) {
          o[prevX] = chunk;
        } else {
          o[prevX].push(chunk);
        }
        this.push(group);
        next();
      }
    }));
    return this;
  }

  _transform(chunk, enc, next) {
    next(null, chunk);
  }
}

module.exports = Stream;
