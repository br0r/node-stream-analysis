const {
  Transform,
  Readable,
} = require('stream');

class GroupStream extends Transform {
  constructor(parent, keys, override) {
    super({
      objectMode: true,
    });
    this.group = {};
    this.override = override;
    this.keys = keys;
    this.parent = parent;
  }

  _transform(chunk, enc, next) {
    if (!chunk) return next();
    let o = this.group;
    const keys = this.keys;
    const override = this.override;
    let prevX;
    for (let i = 0; i < keys.length; i += 1) {
      let k = keys[i];
      let x;
      if (typeof k === 'function') {
        x = k(chunk);
      } else {
        x = chunk[k];
      }

      if (!x) {
        return next();
      }

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

    let formattedChunk = chunk;
    if (typeof this._itemMapF === 'function') formattedChunk = this._itemMapF(formattedChunk);
    if (override) {
      o[prevX] = formattedChunk;
    } else {
      o[prevX].push(formattedChunk);
    }

    this.push(this.group);
    next();
  }

  itemMap(f) {
    this._itemMapF = f;
    return this;
  }

  reduce(...args) {
    this.parent.prev = this.parent.prev.pipe(this);
    return this.parent.reduce(...args);
  }

  toJSON(...args) {
    this.parent.prev = this.parent.prev.pipe(this);
    return this.parent.toJSON(...args);
  }

  consume(...args) {
    this.parent.prev = this.parent.prev.pipe(this);
    return this.parent.consume(...args);
  }
}

module.exports = GroupStream;
