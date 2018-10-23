/*
用于API接口信息处理
wrote by tzf, 2017/12/8
modified, 2018/10/15
*/
const searchGraph = require('./searchGraph.js');
const schedule = require("node-schedule");
const moment = require('moment');
const log4js = require('log4js');
const req = require('require-yml');
const config = req('./config/source.yml');
const cacheHandlers = require('./cacheHandlers.js');
const Redis = require('ioredis');
const redis = new Redis(config.lockInfo.redisUrl[0]);
const redis1 = new Redis(config.lockInfo.redisUrl[1]);
const subClient = new Redis(config.redisPubSubInfo.clientUrl[0]);
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
        retryCount: 10,

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
    appenders: {
        console: {
            type: 'console'
        },
        log: {
            type: "dateFile",
            filename: "./logs/log4js_log-",
            pattern: "yyyy-MM-dd.log",
            alwaysIncludePattern: true,
            maxLogSize: config.logInfo.maxLogSize,
            backups: 10
        },
        error: {
            type: "dateFile",
            filename: "./logs/log4js_err-",
            pattern: "yyyy-MM-dd.log",
            alwaysIncludePattern: true,
            maxLogSize: config.logInfo.maxLogSize,
            backups: 10
        },
        errorFilter: {
            type: "logLevelFilter",
            appender: "error",
            level: "error"
        },
    },
    categories: {
        default: { appenders: ['console', 'log', 'errorFilter'], level: 'debug' }
    },
    pm2: true,
    pm2InstanceVar: 'INSTANCE_ID'
});
const logger = log4js.getLogger('arangodb_search');

//定时主动预热paths
let rule = new schedule.RecurrenceRule();
rule.dayOfWeek = [0, new schedule.Range(1, 6)];
rule.hour = config.schedule.hour;
rule.minute = config.schedule.minute;
console.log('定时主动预热paths时间: ' + rule.hour + '时 ' + rule.minute + '分');
// logger.info('定时主动预热paths时间: ' + rule.hour + '时 ' + rule.minute + '分');
//如果该进程的env属于WITH_SCHEDULE(只有1个实例运行)，则执行下面的程序
if (process.env.WITH_SCHEDULE) {
    //单进程执行scheduleJob
    schedule.scheduleJob(rule, function () {
        try {
            redlock.lock(lockResource, lockTTL).then(async function (lock) {
                timingWarmUpPaths('true');
                console.log('process.pid: ' + process.pid + ', process.env.WITH_SCHEDULE: ' + process.env.WITH_SCHEDULE);
                logger.info('process.pid: ' + process.pid + ', process.env.WITH_SCHEDULE: ' + process.env.WITH_SCHEDULE);
                console.log('run timingWarmUpPaths()...');
                logger.info('run timingWarmUpPaths()...');
                redlock.on('clientError', function (err) {
                    console.error('A redis error has occurred:', err);
                });
            });
        } catch (err) {
            console.error(err);
            logger.error(err);
        }
    });

    //单进程执行subscribe
    let channels = [];
    channels = channels.concat(config.redisPubSubInfo.channelsName);
    for (let channel of channels) {
        try {
            subClient.subscribe(channel, function () {
                console.log('subscribe the channel: ', channel);
                logger.info('subscribe the channel: ', channel);
            });
            subClient.on('message', function (channel, message) {
                console.log('process.pid: ' + process.pid + ', receive message： %s from channel： %s', message, channel);
                logger.info('process.pid: ' + process.pid + ', receive message： %s from channel： %s', message, channel);
                let subMesage = config.redisPubSubInfo.subMessage[0];
                if (message === subMesage) {
                    cacheHandlers.deleteWarmUpPathsFromRedis();             //删除预热的paths
                    cacheHandlers.flushCache();                             //清除缓存
                }
            });
        } catch (err) {
            console.error(err);
            logger.error(err);
        }
    }
}

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
            let isPerson = 0;
            if (code.indexOf('P') >= 0) {
                isPerson = 1;
            }
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
                        await searchGraph.queryDirectInvestPath(code, relation, DIDepth, lowWeight, highWeight, lowFund, highFund, lowSubAmountRMB, highSubAmountRMB, isExtra, isBranches, surStatus, isPerson);
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
        if (!DIDepth) DIDepth = 2;
        if (!relation) relation = 'invests';                               //默认invests关系
        let isExtra = 0;                                                   //默认isExtra为0,表示查询时过滤没有机构代码的节点
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
                let queryResult = null;
                //判断code是否自然人的personalCode
                let isPerson = 0;
                if (code.indexOf('P') >= 0) {
                    isPerson = 1;
                }
                queryResult = await searchGraph.queryDirectInvestPath(code, relation, DIDepth, lowWeight, highWeight, lowFund, highFund, lowSubAmountRMB, highSubAmountRMB, isExtra, isBranches, surStatus, isPerson);

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
        // let returnBranches = request.query.returnBranches;                 //是否返回分支机构
        // let surStatus = request.query.surStatus;                           //公司续存状态
        // if (!surStatus) surStatus = 1;                                     //默认surStatus为1 
        if (lowWeight) lowWeight = parseFloat(lowWeight);
        if (highWeight) highWeight = parseFloat(highWeight);
        if (lowFund) lowFund = parseFloat(lowFund);
        if (highFund) highFund = parseFloat(highFund);
        if (lowSubAmountRMB) lowSubAmountRMB = parseFloat(lowSubAmountRMB);
        if (highSubAmountRMB) highSubAmountRMB = parseFloat(highSubAmountRMB);
        if (!DIBDepth) DIBDepth = 2;
        if (!relation) relation = 'invests';                               //默认invests关系
        let isExtra = 0;                                                   //默认isExtra为0,表示查询时过滤没有机构代码的节点
        if (returnNoCodeNodes == "false" || !returnNoCodeNodes) {          //不返回无机构代码的nodes, 即isExtra=0的nodes
            isExtra = 0;
        } else if (returnNoCodeNodes == "true") {                            //返回无机构代码的nodes, 即isExtra=1和isExtra=0的nodes
            isExtra = 1;
        } else {
            isExtra = 0;
        }
        // let isBranches = 0;                                               //默认isBranches为0，表示查询时过滤分支机构
        // if (returnBranches == "false" || !returnBranches) {
        //     isBranches = 0;
        // } else if (returnBranches == "true") {
        //     isBranches = 1;
        // } else {
        //     isBranches = 0;
        // }

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
                let queryResult = await searchGraph.queryDirectInvestedByPath(code, relation, DIBDepth, lowWeight, highWeight, lowFund, highFund, lowSubAmountRMB, highSubAmountRMB, isExtra);
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

    //直接投资关系路径
    queryInvestPathInfo: async function (request, reply) {
        let codeOne = request.query.from;
        let codeTwo = request.query.to;
        let IVDepth = request.query.investPathDepth;
        let lowWeight = request.query.lowWeight;                           //最低投资比例
        let highWeight = request.query.highWeight;                         //最高投资比例
        if (lowWeight) lowWeight = parseFloat(lowWeight);
        if (highWeight) highWeight = parseFloat(highWeight);
        if (!IVDepth) IVDepth = config.pathDepth.IVDepth;
        let pathType = request.query.pathType;                             //默认返回的路径方式为invests
        if (!pathType) pathType = 'invests';
        let user = request.query.username;                                 //调用接口的用户信息
        let depth = parseInt(request.query.depth);                         //对外接口传入的字段
        if (!isNaN(depth)) {
            if (depth > config.pathDepth.IVDepth) {
                depth = config.pathDepth.IVDepth;
            }
            IVDepth = depth;
        }
        if (!user) {
            user = 'unknown';
        }
        try {
            if (lowWeight && highWeight && lowWeight > highWeight) {
                return reply.response({ ok: -1, message: 'highWeight must >= lowWeight!' });
            }
            let res = null;
            if (codeOne && codeTwo) {
                if (codeOne == codeTwo) {
                    console.error('from can not be same as to!');
                    logger.error('from can not be same as to!');
                    return reply.response(errorResp(errorCode.ARG_ERROR, 'from/to参数不能相同!'));
                }
                else if (codeOne != codeTwo) {
                    let now = Date.now();
                    console.log('user: ' + user + ', queryInvestPath  from: ' + codeOne + ', to: ' + codeTwo);
                    logger.info('user: ' + user + ', queryInvestPath  from: ' + codeOne + ', to: ' + codeTwo);
                    searchGraph.queryInvestPath(codeOne, codeTwo, IVDepth, lowWeight, highWeight, pathType)
                        .then(res => {
                            let totalQueryCost = Date.now() - now;
                            logger.info(`user: ${user}, from: ${codeOne} to: ${codeTwo}` + " queryInvestPath_totalQueryCost: " + totalQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` user: ${user}, from: ${codeOne} to: ${codeTwo}` + ", queryInvestPath_totalQueryCost: " + totalQueryCost + 'ms');
                            if (!isNaN(depth)) {                                                      //depth不为空时做数据重新封装
                                let newRes = pathHandlers.dataProcess(res);
                                return reply.response({ direction: { from: codeOne, to: codeTwo }, results: newRes });
                            }
                            else {
                                if (!res) {
                                    return reply.response({ direction: { from: codeOne, to: codeTwo }, results: { pathTypeOne: "no results!", pathTypeTwo: "no results!", pathTypeThree: "no results!" } });
                                }
                                else if (res) {
                                    return reply.response({ direction: { from: codeOne, to: codeTwo }, results: { pathTypeOne: res.nodeResultOne.pathDetail, pathTypeTwo: res.nodeResultTwo.pathDetail, pathTypeThree: res.nodeResultThree.pathDetail } });
                                }
                            }
                        }).catch(err => {
                            return reply.response({ ok: -1, message: err.message || err });
                        });
                }
            } else if (!codeOne && codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from参数!`));
            } else if (codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少to参数!`));
            } else if (!codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from和to参数!`));
            }

        } catch (err) {
            return reply.response(err);
        }
    },

    //直接被投资关系路径
    queryInvestedByPathInfo: async function (request, reply) {
        let codeOne = request.query.from;
        let codeTwo = request.query.to;
        let IVBDepth = request.query.investedByPathDepth;
        let lowWeight = request.query.lowWeight;                           //最低投资比例
        let highWeight = request.query.highWeight;                         //最高投资比例
        if (lowWeight) lowWeight = parseFloat(lowWeight);
        if (highWeight) highWeight = parseFloat(highWeight);
        if (!IVBDepth) IVBDepth = config.pathDepth.IVBDepth;
        let pathType = request.query.pathType;                             //默认返回的路径方式为invests
        if (!pathType) pathType = 'invests';
        let user = request.query.username;                                 //调用接口的用户信息
        let depth = parseInt(request.query.depth);                         //对外接口传入的字段
        if (!isNaN(depth)) {
            if (depth > config.pathDepth.IVBDepth) {
                depth = config.pathDepth.IVBDepth;
            }
            IVBDepth = depth;
        }
        if (!user) {
            user = 'unknown';
        }
        try {
            if (lowWeight && highWeight && lowWeight > highWeight) {
                return reply.response({ ok: -1, message: 'highWeight must >= lowWeight!' });
            }
            let res = null;
            if (codeOne && codeTwo) {
                if (codeOne == codeTwo) {
                    console.error('from can not be same as to!');
                    logger.error('from can not be same as to!');
                    return reply.response(errorResp(errorCode.ARG_ERROR, 'from/to参数不能相同!'));
                }
                else if (codeOne != codeTwo) {
                    let now = Date.now();
                    console.log('user: ' + user + ', queryInvestedByPath  from: ' + codeOne + ', to: ' + codeTwo);
                    logger.info('user: ' + user + ', queryInvestedByPath  from: ' + codeOne + ', to: ' + codeTwo);
                    searchGraph.queryInvestedByPath(codeOne, codeTwo, IVBDepth, lowWeight, highWeight, pathType)
                        .then(res => {
                            let totalQueryCost = Date.now() - now;
                            logger.info(`user: ${user}, from: ${codeOne} to: ${codeTwo}` + " queryInvestedByPath_totalQueryCost: " + totalQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` user: ${user}, from: ${codeOne} to: ${codeTwo}` + ", queryInvestedByPath_totalQueryCost: " + totalQueryCost + 'ms');
                            if (!isNaN(depth)) {                                                      //depth不为空时做数据重新封装
                                let newRes = pathHandlers.dataProcess(res);
                                return reply.response({ direction: { from: codeOne, to: codeTwo }, results: newRes });
                            }
                            else {
                                if (!res) {
                                    return reply.response({ direction: { from: codeOne, to: codeTwo }, results: { pathTypeOne: "no results!", pathTypeTwo: "no results!", pathTypeThree: "no results!" } });
                                }
                                else if (res) {
                                    // let totalPathNum = res.pathDetail.data.pathNum;
                                    // console.log(`from: ${codeOne} to: ${codeTwo}`+ ' queryInvestPath_totalPathNum: ' + totalPathNum);
                                    // logger.info(`from: ${codeOne} to: ${codeTwo}`+ ' queryInvestPath_totalPathNum: ' + totalPathNum);
                                    return reply.response({ direction: { from: codeOne, to: codeTwo }, results: { pathTypeOne: res.nodeResultOne.pathDetail, pathTypeTwo: res.nodeResultTwo.pathDetail, pathTypeThree: res.nodeResultThree.pathDetail } });
                                }
                            }
                        }).catch(err => {
                            return reply.response({ ok: -1, message: err.message || err });
                        });
                }
            } else if (!codeOne && codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from参数!`));
            } else if (codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少to参数!`));
            } else if (!codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from和to参数!`));
            }

        } catch (err) {
            return reply.response(err);
        }
    },

    //共同投资关系路径
    queryCommonInvestPathInfo: async function (request, reply) {
        let codeOne = request.query.from;
        let codeTwo = request.query.to;
        let CIVDepth = request.query.comInvestPathDepth;
        let lowWeight = request.query.lowWeight;                           //最低投资比例
        let highWeight = request.query.highWeight;                         //最高投资比例
        if (lowWeight) lowWeight = parseFloat(lowWeight);
        if (highWeight) highWeight = parseFloat(highWeight);
        if (!CIVDepth) CIVDepth = config.pathDepth.CIVDepth;
        let pathType = request.query.pathType;                             //默认返回的路径方式为invests
        if (!pathType) pathType = 'invests';
        let user = request.query.username;                                 //调用接口的用户信息
        let depth = parseInt(request.query.depth);                         //对外接口传入的字段
        if (!isNaN(depth)) {
            if (depth >  config.pathDepth.CIVDepth) {
                depth =  config.pathDepth.CIVDepth;
            }
            CIVDepth = depth;
        }
        if (!user) {
            user = 'unknown';
        }
        try {
            if (lowWeight && highWeight && lowWeight > highWeight) {
                return reply.response({ ok: -1, message: 'highWeight must >= lowWeight!' });
            }
            let res = null;
            if (codeOne && codeTwo) {
                if (codeOne == codeTwo) {
                    console.error('from can not be same as to!');
                    logger.error('from can not be same as to!');
                    return reply.response(errorResp(errorCode.ARG_ERROR, 'from/to参数不能相同!'));
                }
                else if (codeOne != codeTwo) {
                    let now = Date.now();
                    console.log('user: ' + user + ', queryCommonInvestPath  from: ' + codeOne + ', to: ' + codeTwo);
                    logger.info('user: ' + user + ', queryCommonInvestPath  from: ' + codeOne + ', to: ' + codeTwo);
                    searchGraph.queryCommonInvestPath(codeOne, codeTwo, CIVDepth, lowWeight, highWeight, pathType)
                        .then(res => {
                            let totalQueryCost = Date.now() - now;
                            logger.info(`user: ${user}, from: ${codeOne} to: ${codeTwo}` + " queryCommonInvestPath_totalQueryCost: " + totalQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` user: ${user}, from: ${codeOne} to: ${codeTwo}` + ", queryCommonInvestPath_totalQueryCost: " + totalQueryCost + 'ms');
                            if (!isNaN(depth)) {                                                      //depth不为空时做数据重新封装
                                let newRes = pathHandlers.dataProcess(res);
                                return reply.response({ direction: { from: codeOne, to: codeTwo }, results: newRes });
                            }
                            else {
                                if (!res) {
                                    return reply.response({ direction: { from: codeOne, to: codeTwo }, results: { pathTypeOne: "no results!", pathTypeTwo: "no results!", pathTypeThree: "no results!" } });
                                }
                                else if (res) {
                                    return reply.response({ direction: { from: codeOne, to: codeTwo }, results: { pathTypeOne: res.nodeResultOne.pathDetail, pathTypeTwo: res.nodeResultTwo.pathDetail, pathTypeThree: res.nodeResultThree.pathDetail } });
                                }
                            }
                        }).catch(err => {
                            return reply.response({ ok: -1, message: err.message || err });
                        });
                }
            } else if (!codeOne && codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from参数!`));
            } else if (codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少to参数!`));
            } else if (!codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from和to参数!`));
            }

        } catch (err) {
            return reply.response(err);
        }
    },

    //共同被投资关系路径
    queryCommonInvestedByPathInfo: async function (request, reply) {
        let codeOne = request.query.from;
        let codeTwo = request.query.to;
        let CIVBDepth = request.query.comInvestedByPathDepth;
        let lowWeight = request.query.lowWeight;                           //最低投资比例
        let highWeight = request.query.highWeight;                         //最高投资比例
        if (lowWeight) lowWeight = parseFloat(lowWeight);
        if (highWeight) highWeight = parseFloat(highWeight);
        if (!CIVBDepth) CIVBDepth = config.pathDepth.CIVBDepth;
        let returnNoCodeNodes = request.query.returnNoCodeNodes;           //是否带入没有机构代码的节点查询
        if (returnNoCodeNodes == "false" || !returnNoCodeNodes) {          //不返回无机构代码的nodes, 即isExtra=0的nodes
            isExtra = 0;
        } else if (returnNoCodeNodes == "true") {                          //返回无机构代码的nodes, 即isExtra=1和isExtra=0的nodes
            isExtra = 1;
        } else {
            isExtra = 0;
        }
        let pathType = request.query.pathType;                             //默认返回的路径方式为invests
        if (!pathType) pathType = 'invests';
        let user = request.query.username;                                 //调用接口的用户信息
        let depth = parseInt(request.query.depth);                         //对外接口传入的字段
        if (!isNaN(depth)) {
            if (depth >  config.pathDepth.CIVBDepth) {
                depth =  config.pathDepth.CIVBDepth;
            }
            CIVBDepth = depth;
            isExtra = 1;
        }
        if (!user) {
            user = 'unknown';
        }
        try {
            if (lowWeight && highWeight && lowWeight > highWeight) {
                return reply.response({ ok: -1, message: 'highWeight must >= lowWeight!' });
            }
            let res = null;
            if (codeOne && codeTwo) {
                if (codeOne == codeTwo) {
                    console.error('from can not be same as to!');
                    logger.error('from can not be same as to!');
                    return reply.response(errorResp(errorCode.ARG_ERROR, 'from/to参数不能相同!'));
                }
                else if (codeOne != codeTwo) {
                    let now = Date.now();
                    console.log('user: ' + user + ', queryCommonInvestedByPath  from: ' + codeOne + ', to: ' + codeTwo);
                    logger.info('user: ' + user + ', queryCommonInvestedByPath  from: ' + codeOne + ', to: ' + codeTwo);
                    searchGraph.queryCommonInvestedByPath(codeOne, codeTwo, CIVBDepth, lowWeight, highWeight, isExtra, pathType)
                        .then(res => {
                            let totalQueryCost = Date.now() - now;
                            logger.info(`user: ${user}, from: ${codeOne} to: ${codeTwo}` + " queryCommonInvestedByPath_totalQueryCost: " + totalQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` user: ${user}, from: ${codeOne} to: ${codeTwo}` + ", queryCommonInvestedByPath_totalQueryCost: " + totalQueryCost + 'ms');
                            if (!isNaN(depth)) {                                                      //depth不为空时做数据重新封装
                                let newRes = pathHandlers.dataProcess(res);
                                return reply.response({ direction: { from: codeOne, to: codeTwo }, results: newRes });
                            }
                            else {
                                if (!res) {
                                    return reply.response({ direction: { from: codeOne, to: codeTwo }, results: { pathTypeOne: "no results!", pathTypeTwo: "no results!", pathTypeThree: "no results!" } });
                                }
                                else if (res) {
                                    return reply.response({ direction: { from: codeOne, to: codeTwo }, results: { pathTypeOne: res.nodeResultOne.pathDetail, pathTypeTwo: res.nodeResultTwo.pathDetail, pathTypeThree: res.nodeResultThree.pathDetail } });
                                }
                            }
                        }).catch(err => {
                            return reply.response({ ok: -1, message: err.message || err });
                        });
                }
            } else if (!codeOne && codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from参数!`));
            } else if (codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少to参数!`));
            } else if (!codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from和to参数!`));
            }

        } catch (err) {
            return reply.response(err);
        }
    },

    //高管投资关系路径查询
    queryExecutiveInvestPathInfo: async function (request, reply) {
        let code = request.query.personalCode;
        let user = request.query.username;                                 //调用接口的用户信息
        if (!user) {
            user = 'unknown';
        }
        let surStatus = request.query.surStatus;                           //公司续存状态
        if (!surStatus) surStatus = 0;                                     //默认surStatus为0 
        try {
            let res = null;
            if (code.indexOf('P') < 0) {
                return reply.response({ ok: -1, message: 'input code is not the personalCode, please check it!' });
            }
            else if (code.indexOf('P') >= 0) {
                let now = Date.now();
                console.log(`user: ${user}`+ ', queryExecutiveInvestPath  code: ' + code);
                logger.info(`user: ${user}`+ ', queryExecutiveInvestPath  code: ' + code);
                searchGraph.queryExecutiveInvestPath(code, surStatus)
                    .then(res => {
                        let totalQueryCost = Date.now() - now;
                        logger.info(`user: ${user}`+", queryExecutiveInvestPath_totalQueryCost: " + totalQueryCost + 'ms');
                        console.log("time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") +`user: ${user}`+ ", queryExecutiveInvestPath_totalQueryCost: " + totalQueryCost + 'ms');

                        if (!res) {
                            return reply.response({ code: code, results: "no results!" });
                        }
                        else if (res) {
                            let totalPathNum = res.data.pathNum;
                            console.log('queryExecutiveInvestPath_totalPathNum: ' + totalPathNum);
                            logger.info('queryExecutiveInvestPath_totalPathNum: ' + totalPathNum);
                            return reply.response({ code: code, results: res });
                        }
                    }).catch(err => {
                        return reply.response({ ok: -1, message: err.message || err });
                    });
            } else if (!code) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少code参数!`));
            }
        } catch (err) {
            return reply.response(err);
        }
    },

    //担保关系路径
    queryGuaranteePathInfo: async function (request, reply) {
        let codeOne = request.query.from;
        let codeTwo = request.query.to;
        let GTDepth = request.query.guaranteePathDepth;
        if (!GTDepth) GTDepth = config.pathDepth.GTDepth;
        let user = request.query.username;                                 //调用接口的用户信息
        if (!user) {
            user = 'unknown';
        }
        let depth = parseInt(request.query.depth);                         //对外接口传入的字段
        if (!isNaN(depth)) {
            if (depth >  config.pathDepth.GTDepth) {
                depth =  config.pathDepth.GTDepth;
            }
            GTDepth = depth;
        }
        try {
            let res = null;
            if (codeOne && codeTwo) {
                if (codeOne == codeTwo) {
                    console.error('from can not be same as to!');
                    logger.error('from can not be same as to!');
                    return reply.response(errorResp(errorCode.ARG_ERROR, 'from/to参数不能相同!'));
                }
                else if (codeOne != codeTwo) {
                    let now = Date.now();
                    console.log('user: ' + user + ', queryGuaranteePath  from: ' + codeOne + ', to: ' + codeTwo);
                    logger.info('user: ' + user + ', queryGuaranteePath  from: ' + codeOne + ', to: ' + codeTwo);
                    searchGraph.queryGuaranteePath(codeOne, codeTwo, GTDepth)
                        .then(res => {
                            let totalQueryCost = Date.now() - now;
                            logger.info(`user: ${user}, from: ${codeOne} to: ${codeTwo}` + " queryGuaranteePath_totalQueryCost: " + totalQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` user: ${user}, from: ${codeOne} to: ${codeTwo}` + ", queryGuaranteePath_totalQueryCost: " + totalQueryCost + 'ms');
                            if (!isNaN(depth)) {                                                      //depth不为空时做数据重新封装
                                let newRes = pathHandlers.dataProcess2(res);
                                return reply.response({ direction: { from: codeOne, to: codeTwo }, results: newRes });
                            }
                            else {
                                if (!res) {
                                    return reply.response({ direction: { from: codeOne, to: codeTwo }, results: "no results!" });
                                }
                                else if (res) {
                                    let totalPathNum = res.nodeResultTwo.pathDetail.data.pathNum;
                                    console.log(`from: ${codeOne} to: ${codeTwo}` + ' queryGuaranteePath_totalPathNum: ' + totalPathNum);
                                    logger.info(`from: ${codeOne} to: ${codeTwo}` + ' queryGuaranteePath_totalPathNum: ' + totalPathNum);
                                    return reply.response({ direction: { from: codeOne, to: codeTwo }, results: res.nodeResultTwo.pathDetail });
                                }
                            }
                        }).catch(err => {
                            return reply.response({ ok: -1, message: err.message || err });
                        });
                }
            } else if (!codeOne && codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from参数!`));
            } else if (codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少to参数!`));
            } else if (!codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from和to参数!`));
            }

        } catch (err) {
            return reply.response(err);
        }
    },

    //被担保关系路径
    queryGuaranteedByPathInfo: async function (request, reply) {
        let codeOne = request.query.from;
        let codeTwo = request.query.to;
        let GTBDepth = request.query.guaranteedByPathDepth;
        if (!GTBDepth) GTBDepth = config.pathDepth.GTBDepth;
        let user = request.query.username;                                 //调用接口的用户信息
        if (!user) {
            user = 'unknown';
        }
        let depth = parseInt(request.query.depth);                         //对外接口传入的字段
        if (!isNaN(depth)) {
            if (depth >  config.pathDepth.GTBDepth) {
                depth =  config.pathDepth.GTBDepth;
            }
            GTBDepth = depth;
        }
        try {
            let res = null;
            if (codeOne && codeTwo) {
                if (codeOne == codeTwo) {
                    console.error('from can not be same as to!');
                    logger.error('from can not be same as to!');
                    return reply.response(errorResp(errorCode.ARG_ERROR, 'from/to参数不能相同!'));
                }
                else if (codeOne != codeTwo) {
                    let now = Date.now();
                    console.log('user: ' + user + ', queryGuaranteeedByPath  from: ' + codeOne + ', to: ' + codeTwo);
                    logger.info('user: ' + user + ', queryGuaranteedByPath  from: ' + codeOne + ', to: ' + codeTwo);
                    searchGraph.queryGuaranteedByPath(codeOne, codeTwo, GTBDepth)
                        .then(res => {
                            let totalQueryCost = Date.now() - now;
                            logger.info(`user: ${user}, from: ${codeOne} to: ${codeTwo}` + " queryGuaranteeedByPath_totalQueryCost: " + totalQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` user: ${user}, from: ${codeOne} to: ${codeTwo}` + ", queryGuaranteeedByPath_totalQueryCost: " + totalQueryCost + 'ms');
                            if (!isNaN(depth)) {                                                      //depth不为空时做数据重新封装
                                let newRes = pathHandlers.dataProcess2(res);
                                return reply.response({ direction: { from: codeOne, to: codeTwo }, results: newRes });
                            }
                            else {
                                if (!res) {
                                    return reply.response({ direction: { from: codeOne, to: codeTwo }, results: "no results!" });
                                }
                                else if (res) {
                                    let totalPathNum = res.nodeResultTwo.pathDetail.data.pathNum;
                                    console.log(`from: ${codeOne} to: ${codeTwo}` + ' queryGuaranteeedByPath_totalPathNum: ' + totalPathNum);
                                    logger.info(`from: ${codeOne} to: ${codeTwo}` + ' queryGuaranteeedByPath_totalPathNum: ' + totalPathNum);
                                    return reply.response({ direction: { from: codeOne, to: codeTwo }, results: res.nodeResultTwo.pathDetail });
                                }
                            }

                        }).catch(err => {
                            return reply.response({ ok: -1, message: err.message || err });
                        });
                }
            } else if (!codeOne && codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from参数!`));
            } else if (codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少to参数!`));
            } else if (!codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from和to参数!`));
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
                let isPerson = 0;
                if (code.indexOf('P') >= 0) {
                    isPerson = 1;
                }
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
                            await searchGraph.queryDirectInvestPath(code, relation, DIDepth, lowWeight, highWeight, lowFund, highFund, lowSubAmountRMB, highSubAmountRMB, isExtra, isBranches, surStatus, isPerson);
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