'use strict';

class Pool {
    constructor(uri) {
        this.uri = uri;
    }
    static get cache() {
        return this._cache || (this._cache = new Map());
    }
    static connect(uri) {
        if (!this.cache.has(uri)) {
            this.cache.set(uri, new this(uri));
        }
        return this.cache.get(uri);
    }
};

module.exports = Pool;