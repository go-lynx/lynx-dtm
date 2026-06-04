package dtm

import (
	"github.com/go-lynx/lynx/pkg/factory"
	"github.com/go-lynx/lynx/plugins"
)

// init registers the DTM plugin with the global factory on import.
func init() {
	factory.GlobalTypedFactory().RegisterPlugin(pluginName, confPrefix, func() plugins.Plugin {
		return NewDTMClient()
	})
}
