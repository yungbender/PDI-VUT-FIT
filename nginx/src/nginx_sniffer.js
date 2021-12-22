/**
 * Nginx logger, ran in NJS.
 * Authors: Tomas Sasak (xsasak01), Jakub Frejlach (xfrejl00)
 */
var fs = require("fs");

var log_path = process.env.LOGS_FOLDER;

/**
 * Function is called for each request caught by nginx.
 * Njs exports single log with requested attributes
 * for each request. (This script is run from njs).
 */
function sniff(req) {
    let res = {
        httpVersion: req.httpVersion,
        userAgent: req.headersIn["User-Agent"],
        sourceIp: req.remoteAddress,
        contentType: req.headersIn["Content-Type"],
        contentLength: parseInt(req.headersIn["Content-Length"]),
    };

    fs.writeFileSync(`${log_path}/${req.variables["request_id"]}`, JSON.stringify(res));

    req.return(200, "OK");
};

export default { sniff };
