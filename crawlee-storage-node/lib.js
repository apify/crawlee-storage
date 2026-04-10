const native = require('./index.js');

// Add Symbol.asyncIterator to DatasetItemIterator so users can write:
//   for await (const item of client.iterateItems()) { ... }
native.DatasetItemIterator.prototype[Symbol.asyncIterator] = function () {
    return {
        next: async () => {
            const value = await this.next();
            if (value === null) {
                return { done: true, value: undefined };
            }
            return { done: false, value };
        },
    };
};

// Same for KvsKeyIterator.
native.KvsKeyIterator.prototype[Symbol.asyncIterator] = function () {
    return {
        next: async () => {
            const value = await this.next();
            if (value === null) {
                return { done: true, value: undefined };
            }
            return { done: false, value };
        },
    };
};

module.exports = native;
