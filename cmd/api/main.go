package main

import (
	"go.uber.org/fx"
)

func main() {
	fx.New(
	// fxmodules serão registrados aqui a partir da Fase 5
	).Run()
}
