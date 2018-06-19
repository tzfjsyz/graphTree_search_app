/*用于数据缓存, 数据预热
wrote by tzf, 2018/4/11
*/
const req = require('require-yml');
const config = req('./config/source.yml');
const NodeCache = require("node-cache");
const cache_stdTTL = config.NodeCache.stdTTL;
const cache_checkperiod = config.NodeCache.checkperiod;
const cache_useClones = config.NodeCache.useClones;
console.log('cache_stdTTL: ' + cache_stdTTL + 'ms, cache_checkperiod: ' + cache_checkperiod + 'ms, cache_useClones: ' + cache_useClones);
const myCache = new NodeCache({ stdTTL: cache_stdTTL, checkperiod: cache_checkperiod, useClones: cache_useClones });         //缓存失效时间6h
const log4js = require('log4js');
log4js.configure({
    appenders: {
        'out': {
            type: 'file',         //文件输出
            filename: 'logs/queryDataInfo.log',
            maxLogSize: config.logInfo.maxLogSize
        }
    },
    categories: { default: { appenders: ['out'], level: 'info' } }
});
const logger = log4js.getLogger();
const warmUp_RedisUrl_0 = config.warmUp_RedisInfo.url_0;
const warmUp_RedisUrl_1 = config.warmUp_RedisInfo.url_1;
const warmUp_RedisTTL = config.warmUp_RedisInfo.TTL;
const queryNeo4jCostUp = config.warmUp_Condition.queryNeo4jCost;
const queryNeo4jRecordsUp = config.warmUp_Condition.queryNeo4jRecords;
console.log('warmUp_Condition -- queryNeo4jCostUp: ' + queryNeo4jCostUp + ' ms, queryNeo4jRecordsUp: ' + queryNeo4jRecordsUp + ' records');
const Redis = require('ioredis');
const redis_0 = new Redis(warmUp_RedisUrl_0);
const redis_1 = new Redis(warmUp_RedisUrl_1);
console.log('warmUp_RedisUrl_0: ' + warmUp_RedisUrl_0 + ', warmUp_RedisUrl_01: ' + warmUp_RedisUrl_1 + '\n'
    + 'warmUp_RedisTTL: ' + warmUp_RedisTTL + 's' + ', redis_0 connect status: ' + redis_0.status + ', redis_1 connect status: ' + redis_1.status);
const Client = require('dict-client');
const client = new Client(config.dictionaryServer.host, config.dictionaryServer.port);

let cacheHandlers = {
    //set cache
    setCache: function (key, value) {
        let res = myCache.set(key, value);
        console.info('set the cache status: ' + res);
        logger.info('set the cache status: ' + res);
    },

    //get cache
    getCache: async function (key) {
        let res = null;
        return new Promise((resolve, reject) => {
            try {
                myCache.get(key, function (err, value) {
                    if (!err) {
                        if (value == undefined) {
                            console.log(`can not get the cache key: ${key}`);
                            logger.info(`can not get the cache key: ${key}`);
                            return resolve(null);
                        } else {
                            res = value;
                            console.log(`get the cache key: ${key}`);
                            logger.info(`get the cache key: ${key}`);
                            return resolve(res);
                        }
                    }
                });
            } catch (err) {
                console.error('getCacheError: '+err);
                logger.error('getCacheError: '+err);
                return reject(err);
            }
        });
    },

    //将需要预热的from/to/depth/realtion存到Redis中
    setWarmUpConditionsToRedis: async function (key, field, value) {
        // redis_0.mset(new Map(array), 'EX', warmUp_RedisTTL);                                           

        redis_0.hset(key, field, value);

        console.log('setWarmUpConditionsToRedis, the key is: ' + key);
    },

    //获取redis中的预热的from/to/depth/realtion数据
    getWarmUpConditionsFromRedis: function (key, field) {
        return new Promise(async (resolve, reject) => {
            redis_0.hget(key, field, function (err, res) {
                if (!err) {
                    if (res == null) {
                        return resolve(null);
                    }
                    else if (res != null) {
                        return resolve(res);
                    }
                }
                else if (err) {
                    console.error('getWarmUpConditionsFromRedisError: ' +err);
                    logger.error('getWarmUpConditionsFromRedisError: ' +err);
                    return reject(err);
                }
            });
        });
    },

    //删除conditionsField
    deleteWarmUpConditionsField: async function (key, field) {
        redis_0.hdel(key, field);
        console.log('deleteWarmUpConditionsField, the key is: ' + key + ', the field is: ' + field);
        logger.info('deleteWarmUpConditionsField, the key is: ' + key + ', the field is: ' + field);
    },

    //查询所有的conditionsKey对应的conditionsField
    findWarmUpConditionsField: function (key) {
        return new Promise(async (resolve, reject) => {
            redis_0.hkeys(key, function (err, res) {
                if (!err) {
                    if (res != null) {
                        return resolve(res);
                    }
                    else if (res == null) {
                        return resolve(null);
                    }
                }
                else if (err) {
                    console.error('findWarmUpConditionsFieldError: ' +err);
                    logger.error('findWarmUpConditionsFieldError: ' +err);
                    return reject(err);
                }
            });
        });
    },

    //主动预热path数据到redis中
    setWarmUpPathsToRedis: async function (key, value) {
        redis_1.set(key, value, 'EX', warmUp_RedisTTL);                                           //EX设置key的生存时间,单位s
        console.log('warmUpPathToRedis, the key is: ' + key);
        logger.info('warmUpPathToRedis, the key is: ' + key);
    },

    //获取redis中的预热path数据
    getWarmUpPathsFromRedis: function (key) {
        return new Promise(async (resolve, reject) => {
            redis_1.get(key, function (err, res) {
                if (!err) {
                    if (res != null) {
                        return resolve(res);
                    }
                    else if (res == null) {
                        return resolve(null);
                    }
                }
                else if (err) {
                    console.error('getWarmUpPathsFromRedisError: ' +err);
                    logger.error('getWarmUpPathsFromRedisError: ' +err);
                    return reject(err);
                }
            });
        });
    },

    //记录数据更新的信息
    saveContext: async function (id, ctx) {
        let ctx_id = `ctx_${id}`;
        let res = await redis_1.set(ctx_id, JSON.stringify(ctx));
        return res;
    },

    //读取数据更新的信息
    getContext: async function (id) {
        let ctx_id = `ctx_${id}`;
        let res = await redis_1.get(ctx_id);
        if (res) {
            return JSON.parse(res);
        } else {
            return {};
        }
    },

    //批量查询数据字典获取allNames
    getAllNames: async function (codes) {
        return new Promise((resolve, reject) => {
            let now = Date.now();
            client.batchLookup('ITCODE10TOFULL', codes)
                .then(res => {
                    let queryNamesCost = Date.now() - now;
                    console.log('queryNamesCost: ', +queryNamesCost + 'ms');
                    logger.info('queryNamesCost: ', +queryNamesCost + 'ms');
                    return resolve(res);
                    // return res;
                }).catch(err => {
                    console.error('lookUpDictError: '+err);
                    logger.error('lookUpDictError: '+err);
                    return reject(null);
                });
        });
    }

}

module.exports = cacheHandlers;