/*
用于对外接口数据的重新封装
wrote by tzf, 2018/8/31
*/

pathHandlers = {
    //投资关系路径数据处理
    dataProcess: function (paths) {
        let pathResult = {
            pathDetail: [],
            pathCount: 0,
            pathTypeName: null,
            names: [],
            codes: []
        }
        let pathDetails = paths.nodeResultOne.pathDetail;
        for (let subPathDetail of pathDetails.data.pathDetail) {
            let isMainPath = 0;
            if (null != subPathDetail.isMainPath) {
                isMainPath = subPathDetail.isMainPath;
            }
            if (isMainPath == 1 || null == isMainPath) {
                let pathObj = {
                    eachPath: []
                }
                pathObj.eachPath = subPathDetail.path
                pathResult.pathDetail.push(pathObj);
            }
        }
        pathResult.pathCount = pathDetails.data.pathNum;
        pathResult.pathTypeName = pathDetails.typeName;
        pathResult.names = pathDetails.names;
        pathResult.codes = pathDetails.codes;
        return pathResult;
    },
    //担保关系路径数据处理
    dataProcess2: function (paths) {
        let pathResult = {
            pathDetail: [],
            pathCount: 0,
            pathTypeName: null,
            names: [],
            codes: []
        }
        if (null != paths.nodeResultTwo && null != paths.nodeResultTwo.pathDetail) {
            let pathDetails = paths.nodeResultTwo.pathDetail;
            for (let subPathDetail of pathDetails.data.pathDetail) {
                let pathObj = {
                    eachPath: []
                }
                pathObj.eachPath = subPathDetail.path
                pathResult.pathDetail.push(pathObj);
            }
            pathResult.pathCount = pathDetails.data.pathNum;
            pathResult.pathTypeName = pathDetails.pathTypeName;
            pathResult.names = pathDetails.names;
            pathResult.codes = pathDetails.codes;
        }
        return pathResult;
    }

}


module.exports = pathHandlers;