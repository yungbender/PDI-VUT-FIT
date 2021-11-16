import { Kafka } from 'kafkajs';
import { promises as afs } from 'fs';
import * as fs from 'fs';


const kafka = new Kafka({
    clientId: "nginx-exporter",
    brokers: [`${process.env.KAFKA_BROKER}:${process.env.KAFKA_BROKER_PORT}`]
});

const logsLocation = "/tmp/nginx_logs"
const fileCallback = async (eventType: string, filename: string) => {
    if(eventType !== "change")
        return;

    console.log(`Processing log ${filename}`);

    let file = await afs.readFile(`${logsLocation}/${filename}`);

    let producer = await kafka.producer();

    await producer.send({
        topic: "stark-ingress",
        messages: [
            {value: file}
        ],
    });
};

const main = () => {
    console.log(`Starting nginx logs exporter.`)
    // setup listener for logs folder with logs generated by nginx njs
    fs.watch(logsLocation, {
        persistent: true,
        recursive: false,
        encoding: "utf-8",
    }, fileCallback);
};

main();

export default { main };
