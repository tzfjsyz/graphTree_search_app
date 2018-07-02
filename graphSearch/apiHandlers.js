/*
用于API接口信息处理
wrote by tzf, 2017/12/8
*/
const searchGraph = require('./searchGraph.js');
const schedule = require("node-schedule");
const moment = require('moment');
const log4js = require('log4js');
// const Promise = require('bluebird');
const req = require('require-yml');
const config = req('./config/source.yml');
// const path = require('path');
const cacheHandlers = require('./cacheHandlers.js');
// const pathTreeHandlers = require('./pathTreeHandlers.js');
// const getWarmUpQueryData = require('./getWarmUpQueryData.js');
// const sleep = require('system-sleep');
const Redis = require('ioredis');
const redis = new Redis(config.lockInfo.redisUrl[0]);
const redis1 = new Redis(config.lockInfo.redisUrl[1]);
const Redlock = require('redlock');
const lockResource = config.lockInfo.resource[0];
const lockTTL = config.lockInfo.TTL[0];
console.log('lockResource: ' + lockResource + ', lockTTL: ' + lockTTL + ' ms');
let redlock = new Redlock(
    // you should have one client for each independent redis node
    // or cluster
    [redis, redis1],
    {
        // the expected clock drift; for more details
        // see http://redis.io/topics/distlock
        driftFactor: 0.01, // time in ms

        // the max number of times Redlock will attempt
        // to lock a resource before erroring
        retryCount: 1000,

        // the time in ms between attempts
        retryDelay: 10000, // time in ms

        // the max time in ms randomly added to retries
        // to improve performance under high contention
        // see https://www.awsarchitectureblog.com/2015/03/backoff.html
        retryJitter: 200 // time in ms
    }
);


const errorCode = {
    ARG_ERROR: {
        code: -100,
        msg: "请求参数错误"
    },
    NOTSUPPORT: {
        code: -101,
        msg: "不支持的参数"
    },
    INTERNALERROR: {
        code: -200,
        msg: "服务内部错误"
    },
    NOMATCHINGDATA: {
        code: -505,
        msg: "无匹配数据"
    }
}

function errorResp(err, msg) {
    return { ok: err.code, error: msg || err.msg };
}

log4js.configure({
    // appenders: {
    //     'out': {
    //         type: 'file',         //文件输出
    //         filename: 'logs/queryDataInfo.log',
    //         maxLogSize: config.logInfo.maxLogSize
    //     }
    // },
    // categories: { default: { appenders: ['out'], level: 'info' } }
    appenders: {
        console: {
            type: 'console'
        },
        log: {
            type: "dateFile",
            filename: "./logs/log4js_log-",
            pattern: "yyyy-MM-dd.log",
            alwaysIncludePattern: true,
            maxLogSize: config.logInfo.maxLogSize
        },
        error: {
            type: "dateFile",
            filename: "./logs/log4js_err-",
            pattern: "yyyy-MM-dd.log",
            alwaysIncludePattern: true,
            maxLogSize: config.logInfo.maxLogSize
        },
        errorFilter: {
            type: "logLevelFilter",
            appender: "error",
            level: "error"
        },
    },
    categories: {
        default: { appenders: ['console', 'log', 'errorFilter'], level: 'info' }
    },
    pm2: true,
    pm2InstanceVar: 'INSTANCE_ID'
});
const logger = log4js.getLogger('graphTree_search_app');

//定时主动预热paths
let rule = new schedule.RecurrenceRule();
rule.dayOfWeek = [0, new schedule.Range(1, 6)];
rule.hour = config.schedule.hour;
rule.minute = config.schedule.minute;
console.log('定时主动预热paths时间: ' + rule.hour + '时 ' + rule.minute + '分');
// logger.info('定时主动预热paths时间: ' + rule.hour + '时 ' + rule.minute + '分');
schedule.scheduleJob(rule, function () {
    try {
        redlock.lock(lockResource, lockTTL).then(async function (lock) {
            timingWarmUpPaths('true');
            redlock.on('clientError', function (err) {
                console.error('A redis error has occurred:', err);
            });
        });
    } catch (err) {
        console.error(err);
        logger.error(err);
    }
});

//定时触发主动查询需要预热的path数据
async function timingWarmUpPaths(flag) {
    try {
        if (flag) {
            let conditionsResult = await lookUpConditionsFieldInfo(null);
            let conditionsFields = conditionsResult.conditionsField;
            if (conditionsFields.length > 0) {
                for (let subField of conditionsFields) {
                    await addQueryDataInfo(subField);
                }
            }
            else if (conditionsFields.length == 0) {
                console.log('no paths to warm up!');
            }
        }
    } catch (err) {
        console.error(err);
        logger.error(err);
    }
}

//内部方法调用, 查询所有的预热数据的key对应的field
async function lookUpConditionsFieldInfo(conditionsKey) {
    try {
        if (!conditionsKey) conditionsKey = config.redisKeyName.warmUpITCodes;
        let conditionsField = await cacheHandlers.findWarmUpConditionsField(conditionsKey);
        if (!conditionsField) {
            return ({ conditionsKey: conditionsKey, conditionsField: [], fieldsNum: 0 });
        }
        else if (conditionsField) {
            return ({ conditionsKey: conditionsKey, conditionsField: conditionsField, fieldsNum: conditionsField.length });
        }
    } catch (err) {
        console.error(err);
        logger.error(err);
        return (err);
    }
}

//内部方法调用, 添加需要预加热的查询条件信息
async function addQueryDataInfo(code) {
    let DIDepth = config.pathDepth.DIDepth;
    let DIBDepth = config.pathDepth.DIBDepth;
    let isExtra = 0;                                                       //表示查询时过滤没有机构代码的节点
    let returnNoCodeNodes = config.defaultQueryParams.returnNoCodeNodes;
    if (returnNoCodeNodes && returnNoCodeNodes == true) {
        isExtra = 1;
    }
    else if (!returnNoCodeNodes) {
        isExtra = 1;
    }
    let lowWeight = config.defaultQueryParams.lowWeight;
    let highWeight = config.defaultQueryParams.highWeight;
    if (!lowWeight) lowWeight = 0;
    if (!highWeight) highWeight = 100;
    let lowFund = config.defaultQueryParams.lowFund;
    let highFund = config.defaultQueryParams.highFund;
    let lowSubAmountRMB = config.defaultQueryParams.lowSubAmountRMB;
    let highSubAmountRMB = config.defaultQueryParams.highSubAmountRMB;
    if (!lowSubAmountRMB) lowSubAmountRMB = 0;
    if (!highSubAmountRMB) highSubAmountRMB = 100000000000000000;
    let isBranches = 0;                                                   //默认isBranches为0，表示查询时过滤分支机构
    let returnBranches = config.defaultQueryParams.returnBranches;
    if (returnBranches && returnBranches == true) {
        isBranches = 1;
    }
    let surStatus = config.defaultQueryParams.surStatus;
    if (!surStatus) {
        surStatus = 1;
    }
    else if (surStatus && surStatus != 1) {
        surStatus = 0;
    }

    try {
        if (lowWeight && highWeight && lowWeight > highWeight) {
            console.error({ ok: -1, message: 'highWeight must >= lowWeight!' });
            logger.error({ ok: -1, message: 'highWeight must >= lowWeight!' });
        }
        if (lowFund && highFund && lowFund > highFund) {
            console.error({ ok: -1, message: 'highFund must >= lowFund!' });
            logger.error({ ok: -1, message: 'highFund must >= lowFund!' });
        }
        if (lowSubAmountRMB && highSubAmountRMB && lowSubAmountRMB > highSubAmountRMB) {
            console.error({ ok: -1, message: 'highSubAmount must >= lowSubAmount!' });
            logger.error({ ok: -1, message: 'highSubAmount must >= lowSubAmount!' });
        }
        if (code) {
            console.log('warmUpData  code: ' + code);
            logger.info('warmUpData  code: ' + code);
            //1、先将符合预热条件的数据存到redis中
            let conditionsKey = config.redisKeyName.warmUpITCodes;
            let conditionsField = code;
            let conditionsValue = { ITCode: code, depth: [DIDepth, DIBDepth], modes: [0, 1], relations: ['invests', 'guarantees'] };                            //0代表DirectInvest，1代表DirectInvestedBy
            cacheHandlers.setWarmUpConditionsToRedis(conditionsKey, conditionsField, JSON.stringify(conditionsValue));
            //2、再将先前符合预热条件的conditionsValue作为key去筛选path, path作为value存入redis
            for (let subMode of conditionsValue.modes) {
                if (subMode == 0) {
                    for (let subRelation of conditionsValue.relations) {
                        let relation = subRelation;
                        await searchGraph.queryDirectInvestPath(code, relation, DIDepth, lowWeight, highWeight, lowFund, highFund, lowSubAmountRMB, highSubAmountRMB, isExtra, isBranches, surStatus);
                    }
                }
                else if (subMode == 1) {
                    for (let subRelation of conditionsValue.relations) {
                        let relation = subRelation;
                        await searchGraph.queryDirectInvestedByPath(code, relation, DIBDepth, lowWeight, highWeight, lowFund, highFund, lowSubAmountRMB, highSubAmountRMB, isExtra, isBranches);
                    }
                }
            }
            console.log('addWarmUpQueryDataInfo: ok');
            logger.info('addWarmUpQueryDataInfo: ok');

        } else if (!code) {
            console.error(errorResp(errorCode.ARG_ERROR, `缺少code参数!`));
            logger.error(errorResp(errorCode.ARG_ERROR, `缺少code参数!`));
        }
    } catch (err) {
        console.error(err);
        logger.error(err);
    }

}

let apiHandlers = {

    //单个企业直接投资关系路径查询
    queryDirectInvestPathInfo: async function (request, reply) {
        let code = request.query.code;
        let DIDepth = request.query.directInvestPathDepth;
        let lowWeight = request.query.lowWeight;                           //最低投资比例
        let highWeight = request.query.highWeight;                         //最高投资比例
        let lowFund = request.query.lowFund;                               //最低注册资金
        let highFund = request.query.highFund;                             //最高注册资金
        let relation = request.query.relation;                             //传入关系类型:invests/guarantees
        let returnNoCodeNodes = request.query.returnNoCodeNodes;           //是否带入没有机构代码的节点查询
        let lowSubAmountRMB = request.query.lowSubAmountRMB;               //最低认缴金额
        let highSubAmountRMB = request.query.highSubAmountRMB;             //最高认缴金额
        let returnBranches = request.query.returnBranches;                 //是否返回分支机构
        let surStatus = request.query.surStatus;                           //公司续存状态
        if (!surStatus) surStatus = 1;                                     //默认surStatus为1 
        if (lowWeight) lowWeight = parseFloat(lowWeight);
        if (highWeight) highWeight = parseFloat(highWeight);
        if (lowFund) lowFund = parseFloat(lowFund);
        if (highFund) highFund = parseFloat(highFund);
        if (lowSubAmountRMB) lowSubAmountRMB = parseFloat(lowSubAmountRMB);
        if (highSubAmountRMB) highSubAmountRMB = parseFloat(highSubAmountRMB);
        if (!lowWeight) lowWeight = 0;                                     //默认最低投资比例为0
        if (!highWeight) highWeight = 100;                                 //默认最高投资比例为100
        if (!DIDepth) DIDepth = 2;
        if (!relation) relation = 'invests';                               //默认invests关系
        let isExtra = 0;                                                   //默认isExtra为0,表示查询时过滤没有机构代码的节点
        if (!lowSubAmountRMB) lowSubAmountRMB = 0;                         //默认最低投资比例为0
        if (!highSubAmountRMB) highSubAmountRMB = 100000000000000000;      //默认最低投资比例为100000000000000000
        if (returnNoCodeNodes == "false" || !returnNoCodeNodes) {          //不返回无机构代码的nodes, 即isExtra=0的nodes
            isExtra = 0;
        } else if (returnNoCodeNodes == "true") {                          //返回无机构代码的nodes, 即isExtra=1和isExtra=0的nodes
            isExtra = 1;
        } else {
            isExtra = 0;
        }
        let isBranches = 0;                                               //默认isBranches为0，表示查询时过滤分支机构
        if (returnBranches == "false" || !returnBranches) {
            isBranches = 0;
        } else if (returnBranches == "true") {
            isBranches = 1;
        } else {
            isBranches = 0;
        }

        try {
            if (lowWeight && highWeight && lowWeight > highWeight) {
                return reply.response({ ok: -1, message: 'highWeight must >= lowWeight!' });
            }
            if (lowFund && highFund && lowFund > highFund) {
                return reply.response({ ok: -1, message: 'highFund must >= lowFund!' });
            }
            if (lowSubAmountRMB && highSubAmountRMB && lowSubAmountRMB > highSubAmountRMB) {
                return reply.response({ ok: -1, message: 'highSubAmount must >= lowSubAmount!' });
            }
            if (code) {
                let now = Date.now();
                console.log('queryDirectInvestPath code:' + code);
                logger.info('queryDirectInvestPath code:' + code);
                let isWarmUp = false;
                let queryResult = null;
                let personalCode = null;
                //判断code是否自然人的personalCode
                if (code.slice(0, 1) == 'P') {
                    personalCode = code;
                    queryCode = parseInt(code.replace(/P/g, ''));
                    queryResult = await searchGraph.queryDirectInvestPath(queryCode, relation, DIDepth, lowWeight, highWeight, lowFund, highFund, lowSubAmountRMB, highSubAmountRMB, isExtra, isBranches, surStatus, personalCode);
                } else {
                    queryResult = await searchGraph.queryDirectInvestPath(code, relation, DIDepth, lowWeight, highWeight, lowFund, highFund, lowSubAmountRMB, highSubAmountRMB, isExtra, isBranches, surStatus, personalCode);
                }
                let totalQueryCost = Date.now() - now;
                logger.info(`${code} queryDirectInvestPath_totalQueryCost: ` + totalQueryCost + 'ms');
                console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + `, ${code} queryDirectInvestPath_totalQueryCost: ` + totalQueryCost + 'ms');
                if (!queryResult.error) {
                    if (queryResult.hasOwnProperty('subLeafNum') && queryResult.subLeafNum == 0 && queryResult.subLeaf.length == 0) {
                        console.log('queryDirectInvestedByPath_totalPathNum:' + queryResult.subLeafNum);
                        logger.info('queryDirectInvestedByPath_totalPathNum:' + queryResult.subLeafNum);
                        return reply.response({ code: code, results: [] });
                    }
                    else if (queryResult.hasOwnProperty('subLeafNum') && queryResult.subLeafNum > 0 && queryResult.subLeaf.length > 0) {
                        let subLeafs = queryResult.subLeaf;
                        return reply.response({ code: code, results: subLeafs });
                    }
                }
                else if (queryResult.hasOwnProperty('error') && queryResult.error) {
                    console.log('code error:' + queryResult.error);
                    logger.info('code error:' + queryResult.error);
                    return reply.response({ code: code, error: queryResult.error });
                }

            } else if (!code) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少code参数!`));
            }
        } catch (err) {
            return reply.response(err);
        }
    },

    //单个企业直接被投资关系路径查询
    queryDirectInvestedByPathInfo: async function (request, reply) {
        let code = request.query.code;
        let DIBDepth = request.query.directInvestedByPathDepth;
        let lowWeight = request.query.lowWeight;                           //最低投资比例
        let highWeight = request.query.highWeight;                         //最高投资比例
        let lowFund = request.query.lowFund;                               //最低注册资金
        let highFund = request.query.highFund;                             //最高注册资金
        let relation = request.query.relation;                             //传入关系类型:invests/guarantees
        let returnNoCodeNodes = request.query.returnNoCodeNodes;           //是否带入没有机构代码的节点查询
        let lowSubAmountRMB = request.query.lowSubAmountRMB;               //最低认缴金额
        let highSubAmountRMB = request.query.highSubAmountRMB;             //最高认缴金额
        let returnBranches = request.query.returnBranches;                 //是否返回分支机构
        // let surStatus = request.query.surStatus;                           //公司续存状态
        // if (!surStatus) surStatus = 1;                                     //默认surStatus为1 
        if (lowWeight) lowWeight = parseFloat(lowWeight);
        if (highWeight) highWeight = parseFloat(highWeight);
        if (lowFund) lowFund = parseFloat(lowFund);
        if (highFund) highFund = parseFloat(highFund);
        if (lowSubAmountRMB) lowSubAmountRMB = parseFloat(lowSubAmountRMB);
        if (highSubAmountRMB) highSubAmountRMB = parseFloat(highSubAmountRMB);
        if (!lowWeight) lowWeight = 0;                                     //默认最低投资比例为0
        if (!highWeight) highWeight = 100;                                 //默认最高投资比例为100
        if (!DIBDepth) DIBDepth = 2;
        if (!relation) relation = 'invests';                               //默认invests关系
        let isExtra = 0;                                                   //默认isExtra为0,表示查询时过滤没有机构代码的节点
        if (!lowSubAmountRMB) lowSubAmountRMB = 0;                         //默认最低投资比例为0
        if (!highSubAmountRMB) highSubAmountRMB = 100000000000000000;      //默认最低投资比例为100000000000000000
        if (returnNoCodeNodes == "false" || !returnNoCodeNodes) {          //不返回无机构代码的nodes, 即isExtra=0的nodes
            isExtra = 0;
        } else if (returnNoCodeNodes == "true") {                            //返回无机构代码的nodes, 即isExtra=1和isExtra=0的nodes
            isExtra = 1;
        } else {
            isExtra = 0;
        }
        let isBranches = 0;                                               //默认isBranches为0，表示查询时过滤分支机构
        if (returnBranches == "false" || !returnBranches) {
            isBranches = 0;
        } else if (returnBranches == "true") {
            isBranches = 1;
        } else {
            isBranches = 0;
        }

        try {
            if (lowWeight && highWeight && lowWeight > highWeight) {
                return reply.response({ ok: -1, message: 'highWeight must >= lowWeight!' });
            }
            if (lowFund && highFund && lowFund > highFund) {
                return reply.response({ ok: -1, message: 'highFund must >= lowFund!' });
            }
            if (lowSubAmountRMB && highSubAmountRMB && lowSubAmountRMB > highSubAmountRMB) {
                return reply.response({ ok: -1, message: 'highSubAmount must >= lowSubAmount!' });
            }
            if (code) {
                let now = Date.now();
                console.log('queryDirectInvestedByPath code:' + code);
                logger.info('queryDirectInvestedByPath code:' + code);
                let queryResult = await searchGraph.queryDirectInvestedByPath(code, relation, DIBDepth, lowWeight, highWeight, lowFund, highFund, lowSubAmountRMB, highSubAmountRMB, isExtra, isBranches);
                let totalQueryCost = Date.now() - now;
                logger.info(`${code} queryDirectInvestedByPath_totalQueryCost: ` + totalQueryCost + 'ms');
                console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + `, ${code} queryDirectInvestedByPath_totalQueryCost: ` + totalQueryCost + 'ms');
                if (!queryResult.error) {
                    if (queryResult.hasOwnProperty('subLeafNum') && queryResult.subLeafNum == 0 && queryResult.subLeaf.length == 0) {
                        console.log('queryDirectInvestedByPath_totalPathNum:' + queryResult.subLeafNum);
                        logger.info('queryDirectInvestedByPath_totalPathNum:' + queryResult.subLeafNum);
                        return reply.response({ code: code, results: [] });
                    }
                    else if (queryResult.hasOwnProperty('subLeafNum') && queryResult.subLeafNum > 0 && queryResult.subLeaf.length > 0) {
                        let subLeafs = queryResult.subLeaf;
                        return reply.response({ code: code, results: subLeafs });
                    }
                }
                else if (queryResult.hasOwnProperty('error') && queryResult.error) {
                    console.log('code error:' + queryResult.error);
                    logger.info('code error:' + queryResult.error);
                    return reply.response({ code: code, error: queryResult.error });
                }

            } else if (!code) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少code参数!`));
            }

        } catch (err) {
            return reply.response(err);
        }
    },

    //外部接口调用, 添加需要预加热的查询条件信息
    addWarmUpQueryDataInfo: async function (request, reply) {
        let code = request.query.code;
        let DIDepth = request.query.directInvestPathDepth;
        if (DIDepth) {
            DIDepth = parseInt(DIDepth);
        }
        else if (!DIDepth) {
            DIDepth = config.pathDepth.DIDepth;
        }
        let DIBDepth = request.query.directInvestedByPathDepth;
        if (DIBDepth) {
            DIBDepth = parseInt(DIBDepth);
        }
        else if (!DIBDepth) {
            DIBDepth = config.pathDepth.DIBDepth;
        }
        let isExtra = 0;                                                       //表示查询时过滤没有机构代码的节点
        let returnNoCodeNodes = config.defaultQueryParams.returnNoCodeNodes;
        if (returnNoCodeNodes && returnNoCodeNodes == true) {
            isExtra = 1;
        }
        else if (!returnNoCodeNodes) {
            isExtra = 1;
        }
        let lowWeight = config.defaultQueryParams.lowWeight;
        let highWeight = config.defaultQueryParams.highWeight;
        if (!lowWeight) lowWeight = 0;
        if (!highWeight) highWeight = 100;
        let lowFund = config.defaultQueryParams.lowFund;
        let highFund = config.defaultQueryParams.highFund;
        let lowSubAmountRMB = config.defaultQueryParams.lowSubAmountRMB;
        let highSubAmountRMB = config.defaultQueryParams.highSubAmountRMB;
        if (!lowSubAmountRMB) lowSubAmountRMB = 0;
        if (!highSubAmountRMB) highSubAmountRMB = 100000000000000000;
        let isBranches = 0;                                                   //默认isBranches为0，表示查询时过滤分支机构
        let returnBranches = config.defaultQueryParams.returnBranches;
        if (returnBranches && returnBranches == true) {
            isBranches = 1;
        }
        let surStatus = config.defaultQueryParams.surStatus;
        if (!surStatus) {
            surStatus = 1;
        }
        else if (surStatus && surStatus != 1) {
            surStatus = 0;
        }

        try {
            if (lowWeight && highWeight && lowWeight > highWeight) {
                return reply.response({ ok: -1, message: 'highWeight must >= lowWeight!' });
            }
            if (lowFund && highFund && lowFund > highFund) {
                return reply.response({ ok: -1, message: 'highFund must >= lowFund!' });
            }
            if (lowSubAmountRMB && highSubAmountRMB && lowSubAmountRMB > highSubAmountRMB) {
                return reply.response({ ok: -1, message: 'highSubAmount must >= lowSubAmount!' });
            }
            if (code) {
                console.log('warmUpData  code: ' + code);
                logger.info('warmUpData  code: ' + code);
                //1、先将符合预热条件的数据存到redis中
                let conditionsKey = config.redisKeyName.warmUpITCodes;
                let conditionsField = code;
                let conditionsValue = { ITCode: code, depth: [DIDepth, DIBDepth], modes: [0, 1], relations: ['invests', 'guarantees'] };                            //0代表DirectInvest，1代表DirectInvestedBy
                cacheHandlers.setWarmUpConditionsToRedis(conditionsKey, conditionsField, JSON.stringify(conditionsValue));
                //2、再将先前符合预热条件的conditionsValue作为key去筛选path, path作为value存入redis
                for (let subMode of conditionsValue.modes) {
                    if (subMode == 0) {
                        for (let subRelation of conditionsValue.relations) {
                            let relation = subRelation;
                            await searchGraph.queryDirectInvestPath(code, relation, DIDepth, lowWeight, highWeight, lowFund, highFund, lowSubAmountRMB, highSubAmountRMB, isExtra, isBranches, surStatus);
                        }
                    }
                    else if (subMode == 1) {
                        for (let subRelation of conditionsValue.relations) {
                            let relation = subRelation;
                            await searchGraph.queryDirectInvestedByPath(code, relation, DIBDepth, lowWeight, highWeight, lowFund, highFund, lowSubAmountRMB, highSubAmountRMB, isExtra, isBranches);
                        }
                    }
                }
                return reply.response({ addWarmUpQueryDataInfo: 'ok' });

            } else if (!code) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少code参数!`));
            }
        } catch (err) {
            return reply.response(err);
        }

    },

    //删除预加热的查询条件信息
    deleteWarmUpQueryDataInfo: async function (request, reply) {
        let code = request.query.code;
        let DIDepth = request.query.directInvestPathDepth;
        if (DIDepth) {
            DIDepth = parseInt(DIDepth);
        }
        else if (!DIDepth) {
            DIDepth = config.pathDepth.DIDepth;
        }
        let DIBDepth = request.query.directInvestedByPathDepth;
        if (DIBDepth) {
            DIBDepth = parseInt(DIBDepth);
        }
        else if (!DIBDepth) {
            DIBDepth = config.pathDepth.DIBDepth;
        }
        let isExtra = 0;                                                     //表示查询时过滤没有机构代码的节点
        let returnNoCodeNodes = config.defaultQueryParams.returnNoCodeNodes;
        if (returnNoCodeNodes && returnNoCodeNodes == true) {
            isExtra = 1;
        }
        else if (!returnNoCodeNodes) {
            isExtra = 1;
        }
        let lowWeight = config.defaultQueryParams.lowWeight;
        let highWeight = config.defaultQueryParams.highWeight;
        if (!lowWeight) lowWeight = 0;
        if (!highWeight) highWeight = 100;
        let lowFund = config.defaultQueryParams.lowFund;
        let highFund = config.defaultQueryParams.highFund;
        let lowSubAmountRMB = config.defaultQueryParams.lowSubAmountRMB;
        let highSubAmountRMB = config.defaultQueryParams.highSubAmountRMB;
        if (!lowSubAmountRMB) lowSubAmountRMB = 0;
        if (!highSubAmountRMB) highSubAmountRMB = 100000000000000000;
        let isBranches = 0;                                                   //默认isBranches为0，表示查询时过滤分支机构
        let returnBranches = config.defaultQueryParams.returnBranches;
        if (returnBranches && returnBranches == true) {
            isBranches = 1;
        }
        let surStatus = config.defaultQueryParams.surStatus;
        if (!surStatus) {
            surStatus = 1;
        }
        else if (surStatus && surStatus != 1) {
            surStatus = 0;
        }

        try {
            if (lowWeight && highWeight && lowWeight > highWeight) {
                return reply.response({ ok: -1, message: 'highWeight must >= lowWeight!' });
            }
            if (lowFund && highFund && lowFund > highFund) {
                return reply.response({ ok: -1, message: 'highFund must >= lowFund!' });
            }
            if (lowSubAmountRMB && highSubAmountRMB && lowSubAmountRMB > highSubAmountRMB) {
                return reply.response({ ok: -1, message: 'highSubAmount must >= lowSubAmount!' });
            }
            if (code) {
                console.log('deleteWarmUpData  code: ' + code);
                logger.info('deleteWarmUpData  code: ' + code);
                //1、先将符合预热条件的数据存到redis中
                let conditionsKey = config.redisKeyName.warmUpITCodes;
                let conditionsField = code;
                let conditionsValue = await cacheHandlers.getWarmUpConditionsFromRedis(conditionsKey, conditionsField);
                if (!conditionsValue) {
                    console.log('the WarmUpConditions is not in the redis, the conditionsKey is: ' + conditionsKey + ', the conditionsField is: ' + conditionsField);
                    logger.info('the WarmUpConditions is not in the redis, the conditionsKey is: ' + conditionsKey + ', the conditionsField is: ' + conditionsField);
                    return reply.response({ deleteWarmUpData: 'the WarmUpConditions is not exist!' });
                }
                else if (conditionsValue) {
                    cacheHandlers.deleteWarmUpConditionsField(conditionsKey, conditionsField);
                    console.log('the WarmUpConditions delete info, the conditionsKey is: ' + conditionsKey + ', the conditionsField is: ' + conditionsField);
                    logger.info('the WarmUpConditions delete info, the conditionsKey is: ' + conditionsKey + ', the conditionsField is: ' + conditionsField);
                    return reply.response({ deleteWarmUpData: 'ok' });
                }


            } else if (!code) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少code参数!`));
            }
        } catch (err) {
            return reply.response(err);
        }
    },

    //外部接口调用, 查询所有的预热数据的key对应的field
    listWarmUpConditionsFieldInfo: async function (request, reply) {
        try {
            let conditionsKey = request.query.warmUpITCodes;
            if (!conditionsKey) conditionsKey = config.redisKeyName.warmUpITCodes;
            let conditionsField = await cacheHandlers.findWarmUpConditionsField(conditionsKey);
            if (!conditionsField) {
                return reply.response({ conditionsKey: conditionsKey, conditionsField: [], fieldsNum: 0 });
            }
            else if (conditionsField) {
                return reply.response({ conditionsKey: conditionsKey, conditionsField: conditionsField, fieldsNum: conditionsField.length });
            }
        } catch (err) {
            return reply.response(err);
        }
    },

    //外部触发接口主动查询需要预热的path数据
    startWarmUpPaths: async function (request, reply) {
        try {
            redlock.lock(lockResource, lockTTL).then(async function (lock) {
                timingWarmUpPaths('true');
            });
            return reply.response({ ok: 1, info: 'start warming up paths...' });
        } catch (err) {
            return reply.response(err);
        }
    },

    //删除lockResource
    deleteLockResource: async function (request, reply) {
        try {
            let lockResource = request.query.lockResource;
            if (!lockResource) {
                lockResource = config.lockInfo.resource[0];
            }
            redis.del(lockResource);
            redis1.del(lockResource);
            return reply.response({ ok: 1, message: `delete the lock resource: '${lockResource}' succeess!` })

        } catch (err) {
            return reply.response(err);
        }
    },

    //清空缓存
    deleteCacheData: async function (request, reply) {
        try {
            cacheHandlers.flushCache();
            return reply.response({ ok: 1, message: 'flush the cache succeess!' });
        } catch (err) {
            console.error(err);
            logger.error(err);
            return reply.response(err);
        }
    },

    //删除预热的paths数据
    deleteWarmUpPaths: async function (request, reply) {
        try {
            cacheHandlers.deleteWarmUpPathsFromRedis();
            return reply.response({ ok: 1, message: 'delete the warmup paths succeess!' });
        } catch (err) {
            console.error(err);
            logger.error(err);
            return reply.response(err);
        }
    }

}


module.exports = apiHandlers;