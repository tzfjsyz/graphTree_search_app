/*
用于全量更新[tCR0001_V2.0]表中的company的ITCode2信息
wrote by tzf, 2018/12/8
*/
const cacheHandlers = require('./cacheHandlers.js');
const req = require('require-yml');
const Db = require('mssql');
const Mssql = req('./lib/mssql');
const Pool = req('./lib/pool');
const config = req('./config/source.yml');
const log4js = require('log4js');

// log4js.configure({
//     appenders: {
//         'out': {
//             type: 'file',         //文件输出
//             filename: 'logs/updateData.log',
//             maxLogSize: config.logInfo.maxLogSize
//         }
//     },
//     categories: { default: { appenders: ['out'], level: 'info' } }
// });
// const logger = log4js.getLogger();
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

let getWarmUpQueryData = {
    startQueryData: async function (flag) {
        if (flag) {
            try {
                let id = config.redisKeyName.timestamp;
                let i = 1;
                let index = 0;                                  //记录ITCode的key
                let ctx = await cacheHandlers.getContext(id);
                let fetched = 0;
                if (!ctx.last)
                    ctx.last = 0;                  //全量更新置0
                let resultCount = 0;
                let startTime = Date.now();
                let updateInfo = {};
                let updateStatus = 0;

                do {
                    let rows = [];
                    let now = Date.now();

                    let sql = `select top 10000 cast(tmstamp as bigint) as _ts, ITCode2 from [tCR0001_V2.0] WITH(READPAST) 
                                where ITCode2 is not null and flag<> 1 and CR0001_041 = '1' and tmstamp > cast( cast(${ctx.last} as bigint) as binary(8)) order by tmstamp;`;

                    let res = await Mssql.connect(config.mssql).query(sql);
                    let queryCost = Date.now() - now;
                    rows = res.recordset;
                    fetched = rows.length;                                                //每次查询SQL Server的实际记录数
                    let writeCost = 0;
                    if (fetched > 0) {
                        resultCount += fetched;
                        let keyValue = [];
                        // let codes = [];
                        for (let subRows of rows) {
                            let key = `ITCode_${index}`;
                            let value = subRows.ITCode2;
                            // keyValue.push([key, value].splice(","));
                            keyValue.push({ key: key, value: value });
                            index++;
                        }

                        cacheHandlers.setAllITCodesToRedis(config.redisKeyName.allITCodes, keyValue);
                        ctx.last = rows[fetched - 1]._ts;
                        ctx.updatetime = now;
                        ctx.latestUpdated = resultCount;
                        // 保存同步到的位置
                        cacheHandlers.saveContext(id, ctx)
                            .catch(err => console.error(err));
                        if (fetched > 0)
                            logger.info(`get ITCode2 from table: 'tCR0001_V2.0 ' qry:${queryCost} ms; result:${fetched}` + ', 读取次数: ' + i);
                        console.log(`get ITCode2 from table: 'tCR0001_V2.0 ' qry:${queryCost} ms; result:${fetched}` + ', 读取次数: ' + i);
                        i++;
                        //for test
                        // if(i == 10 )
                        //     break;
                    }
                } while (fetched >= 10000);
                let totalCost = Date.now() - startTime;
                let logInfo = '获取 tCR0001_V2.0中ITCode2信息，总耗时: ' + totalCost + ', 更新记录数: ' + resultCount;
                updateStatus = 1;
                updateInfo.status = updateStatus;
                updateInfo.info = logInfo;
                updateInfo.counts = resultCount;
                logger.info(`counts: ` + i++ + `, totalConst :${totalCost} ms; resultCount: ${resultCount}`);
                console.log(logInfo);
                return updateInfo;
            } catch (err) {
                console.error(err);
                logger.error(err);
                return err;
            }
        }
    }
}


module.exports = getWarmUpQueryData;