package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

type Property struct {
	ID               int    `json:"id"`
	URL              string `json:"url"`
	Description      string `json:"description"`
	CityID           int    `json:"city_id"`
	PropertyTypeID   int    `json:"property_type_id"`
	OfficialRegionID int    `json:"official_region_id"`
	Business         string `json:"business"`
	Value            float64 `json:"value"`
}

func main() {
	// Carregar dados JSON
	dadosFile, err := ioutil.ReadFile("../dados.json")
	if err != nil {
		log.Fatal(err)
	}
	var dados []Property
	if err := json.Unmarshal(dadosFile, &dados); err != nil {
		log.Fatal(err)
	}

	// Número de CPUs disponíveis
	numCPUs := os.Getenv("GOMAXPROCS")
	if numCPUs == "" {
		numCPUs = "1"
	}

	// Função para agrupar os dados por múltiplos campos
	grupoDict := agruparDados(dados)
	chavesGrupos := make([]string, 0, len(grupoDict))
	for key := range grupoDict {
		chavesGrupos = append(chavesGrupos, key)
	}
	chunkSize := len(chavesGrupos) / parseInt(numCPUs)
	if len(chavesGrupos)%parseInt(numCPUs) != 0 {
		chunkSize++
	}
	index := 0

	// Fork workers.
	var wg sync.WaitGroup
	for i := 0; i < parseInt(numCPUs); i++ {
		chunk := chavesGrupos[index:min(index+chunkSize, len(chavesGrupos))]
		index += chunkSize

		wg.Add(1)
		go func(chunk []string) {
			defer wg.Done()
			workerResult := make(map[string][]map[string]interface{})
			for _, key := range chunk {
				resultados := compararDescricoes(grupoDict[key])
				if len(resultados) > 0 {
					workerResult[key] = resultados
				}
			}
			sendResults(workerResult)
		}(chunk)
	}

	wg.Wait()
}

func agruparDados(dados []Property) map[string][]Property {
	grupoDict := make(map[string][]Property)
	for _, item := range dados {
		chaveGrupo := fmt.Sprintf("%d_%d_%d_%s", item.CityID, item.PropertyTypeID, item.OfficialRegionID, item.Business)
		grupoDict[chaveGrupo] = append(grupoDict[chaveGrupo], item)
	}
	return grupoDict
}

func compararDescricoes(grupo []Property) []map[string]interface{} {
	repetidos := make([]map[string]interface{}, 0)
	descricaoMap := make(map[string][]int)
	for _, item := range grupo {
		descricaoMap[item.Description] = append(descricaoMap[item.Description], item.ID)
	}

	for _, ids := range descricaoMap {
		if len(ids) > 1 {
			repetidos = append(repetidos, map[string]interface{}{
				"descricao": ids[0],
				"ids":       ids[1:],
			})
		}
	}

	for i := 0; i < len(grupo); i++ {
		for j := i + 1; j < len(grupo); j++ {
			similarity := stringSimilarity(grupo[i].Description, grupo[j].Description)
			if similarity > 0.90 {
				if grupo[i].Value < grupo[j].Value {
					repetidos = append(repetidos, map[string]interface{}{
						"id1":          grupo[i].ID,
						"id2":          grupo[j].ID,
						"similaridade": similarity,
						
					})
				}
			}
		}
	}

	return repetidos
}

func stringSimilarity(s1, s2 string) float64 {
	return 0.0 // Implemente sua lógica de similaridade de strings aqui
}

func sendResults(resultados map[string][]map[string]interface{}) {
	// Simule o envio dos resultados para o processo principal
	time.Sleep(time.Second)
	fmt.Println("Resultados do worker enviados:", resultados)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func parseInt(s string) int {
	val, err := strconv.Atoi(s)
	if err != nil {
		log.Fatal(err)
	}
	return val
}
