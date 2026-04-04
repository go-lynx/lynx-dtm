package dtm

import "github.com/go-lynx/lynx/log"

const (
	sharedPluginResourceName     = pluginName + ".plugin"
	sharedReadinessResourceName  = pluginName + ".readiness"
	sharedHealthResourceName     = pluginName + ".health"
	privateReadinessResourceName = "readiness"
	privateHealthResourceName    = "health"
)

func (d *DTMClient) registerRuntimePluginAlias() {
	if d == nil || d.rt == nil {
		return
	}
	if err := d.rt.RegisterSharedResource(sharedPluginResourceName, d); err != nil {
		log.Warnf("failed to register DTM shared plugin alias: %v", err)
	}
}

func (d *DTMClient) publishRuntimeContract(ready, healthy bool) {
	if d == nil || d.rt == nil {
		return
	}
	for _, item := range []struct {
		name  string
		value any
	}{
		{name: sharedReadinessResourceName, value: ready},
		{name: sharedHealthResourceName, value: healthy},
	} {
		if err := d.rt.RegisterSharedResource(item.name, item.value); err != nil {
			log.Warnf("failed to register DTM shared runtime contract %s: %v", item.name, err)
		}
	}
	if err := d.rt.RegisterPrivateResource(privateReadinessResourceName, ready); err != nil {
		log.Warnf("failed to register DTM private readiness resource: %v", err)
	}
	if err := d.rt.RegisterPrivateResource(privateHealthResourceName, healthy); err != nil {
		log.Warnf("failed to register DTM private health resource: %v", err)
	}
}
