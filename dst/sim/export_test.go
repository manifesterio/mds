package sim

// PlantKeepFiredTimers plants a runtime bug for TestG3CatchesPlantedBug: a
// cancelled or replaced timer that already fired is received anyway.
func PlantKeepFiredTimers(sys *System) { sys.plantKeepFiredTimers = true }
