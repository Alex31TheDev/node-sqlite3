const assert = require('assert');
const sqlite3 = require('..');

describe('sanitize', function() {
    it('sanitizes string literals with SQLite escaping', function() {
        assert.strictEqual(sqlite3.sanitize('simple'), "'simple'");
        assert.strictEqual(sqlite3.sanitize("it's fine", 'string'), "'it''s fine'");
        assert.strictEqual(sqlite3.sanitize(null, 'string'), 'NULL');
    });

    it('sanitizes identifiers with SQLite escaping', function() {
        assert.strictEqual(sqlite3.sanitize('Users', 'identifier'), '"Users"');
        assert.strictEqual(sqlite3.sanitize('weird"name', 'identifier'), '"weird""name"');
        assert.strictEqual(sqlite3.sanitize('Items; DROP TABLE Items', 'identifier'), '"Items; DROP TABLE Items"');
    });

    it('rejects invalid inputs', function() {
        assert.throws(function() {
            sqlite3.sanitize(1);
        }, /Argument 0 must be a string or null/);

        assert.throws(function() {
            sqlite3.sanitize(null, 'identifier');
        }, /Argument 0 must be a string/);

        assert.throws(function() {
            sqlite3.sanitize('value', 'unknown');
        }, /Unsupported sanitize type/);
    });
});
