const cluster = require('node:cluster');
const os = require('node:os');
const process = require('node:process');
const fs = require('fs');
const stringSimilarity = require('string-similarity');

// Número de CPUs disponíveis
const numCPUs = os.cpus().length;

// Carregar dados JSON
const dados = JSON.parse(fs.readFileSync('../dados.json', 'utf8'));


// Função para agrupar os dados por múltiplos campos
function agruparDados(dados) {
    const grupoDict = {};
    dados.forEach(item => {
        const chaveGrupo = `${item.city_id}_${item.property_type_id}_${item.official_region_id}_${item.business}`;
        if (!grupoDict[chaveGrupo]) {
            grupoDict[chaveGrupo] = [];
        }
        grupoDict[chaveGrupo].push(item);
    });
    return grupoDict;
}

function compararDescricoes(grupoDict) {
    const repetidos = {};
    Object.keys(grupoDict).forEach(chaveGrupo => {
        const grupo = grupoDict[chaveGrupo];
        const descricaoMap = {};

        grupo.forEach(item => {
            const descricao = item.description;
            if (!descricaoMap[descricao]) {
                descricaoMap[descricao] = [];
            }
            descricaoMap[descricao].push(item.id);
        });

        Object.keys(descricaoMap).forEach(descricao => {
            const ids = descricaoMap[descricao];
            if (ids.length > 1) {
                if (!repetidos[chaveGrupo]) {
                    repetidos[chaveGrupo] = [];
                }
                //repetidos[chaveGrupo].push({ descricao, ids });
            }
        });

        // Usando string-similarity para encontrar descrições semelhantes
        for (let i = 0; i < grupo.length; i++) {
            for (let j = i + 1; j < grupo.length; j++) {
                const similarity = stringSimilarity.compareTwoStrings(grupo[i].description, grupo[j].description);
                if (similarity > 0.90) {  // Ajuste o limite de similaridade conforme necessário
                    if (!repetidos[chaveGrupo]) {
                        repetidos[chaveGrupo] = [];
                    }
                    if (grupo[i].value < grupo[j].value) {
                        repetidos[chaveGrupo].push({
                            id1: grupo[i].id,
                            id2: grupo[j].id,
                            similaridade: similarity
                        });
                    }
                }
            }
        }
    });
    return repetidos;
}

if (cluster.isPrimary) {
    console.time("tempoExecucao");
    console.log(`Primary ${process.pid} is running`);


    // Agrupar dados uma vez no processo principal
    const grupoDict = agruparDados(dados);
    const chavesGrupos = Object.keys(grupoDict);
    const chunkSize = Math.ceil(chavesGrupos.length / numCPUs);
    const numberGroups = chavesGrupos.length;
    let index = 0;

    // Fork workers.
    for (let i = 0; i < numCPUs; i++) {
        const worker = cluster.fork();
        const chunk = chavesGrupos.slice(index, index + chunkSize).map(key => ({ key, group: grupoDict[key] }));
        worker.send(chunk);
        index += chunkSize;
    }

    const resultadosAll = {};
    let completedWorkers = 0;

    cluster.on('message', (worker, message) => {
        Object.assign(resultadosAll, message);
        completedWorkers++;

        // Calcular a porcentagem de progresso
        const progresso = Math.floor((completedWorkers / numCPUs) * 100);
        console.log(`Progresso: ${progresso}%`);

        if (completedWorkers === numCPUs) {
            console.log('Resutaldo: ', Object.keys(resultadosAll).length);
            const nomeArquivo = 'result_clusters.json';
            try {
                fs.writeFileSync(nomeArquivo, JSON.stringify(resultadosAll), { flag: 'w' });
                console.log(`Conteúdo adicionado ao arquivo '${nomeArquivo}'!`);
                console.timeEnd("tempoExecucao");
            } catch (err) {
                console.error('Erro ao escrever no arquivo:', err);
            }
            //cluster.disconnect();
        }
    });

    cluster.on('exit', (worker, code, signal) => {
        console.log(`Worker ${worker.process.pid} died`);
    });




} else {
    process.on('message', (chunk) => {
        const grupoDict = {};
        chunk.forEach(({ key, group }) => {
            grupoDict[key] = group;
        });
        const resultados = compararDescricoes(grupoDict);
        process.send(resultados);
        //process.exit();
    });
}
