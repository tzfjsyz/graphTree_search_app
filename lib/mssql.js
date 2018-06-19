'use strict';

const Db = require('mssql');
const Pool = require('./pool');


class MssqlPool extends Pool {
    constructor(uri) {
        super(uri);
		let hasParams = false;
		if( uri.indexOf("?") > 0 ){
			hasParams = true;
		}
		if( uri.indexOf("requestTimeout") < 0 ){
			uri += hasParams?"&":"?";
			uri += "requestTimeout=86400000";       //24h
		}
        this.pool = new Db.ConnectionPool(uri);
        this.conn = this.pool.connect();
    }
    query(sql) {
        return this.conn.then(pool => pool.request().query(sql));
    }
    static col(field) {
        var type = field.type.name;
        return {
            name: field.name.toLowerCase(),
            type: Mapping[type],
            Type: type
        };
    }
};

module.exports = MssqlPool;