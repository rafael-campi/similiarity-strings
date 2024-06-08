const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');

if (isMainThread) {
    const worker = new Worker(__filename, {
        workerData: { start: 1 }
    });

    worker.on('message', (msg) => {
        console.log('Message from worker:', msg);
    });

    worker.on('error', (err) => {
        console.error('Worker error:', err);
    });

    worker.on('exit', (code) => {
        if (code !== 0) {
            console.error(`Worker stopped with exit code ${code}`);
        }
    });
} else {
    parentPort.postMessage(workerData.start + 1);
}
