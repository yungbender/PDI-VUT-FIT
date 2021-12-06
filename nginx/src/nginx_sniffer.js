var fs = require("fs");

var log_path = process.env.LOGS_FOLDER;

function sniff(req) {
    let res = {
        httpVersion: req.httpVersion,
        userAgent: req.headersIn["User-Agent"],
        sourceIp: req.remoteAddress,
    };

    fs.writeFileSync(`${log_path}/${req.variables["request_id"]}`, JSON.stringify(res));

    req.return(200, "OK");
};

export default { sniff };