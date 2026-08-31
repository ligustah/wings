package gcp

import (
	"errors"
	"flag"

	"github.com/ligustah/wings"
)

// Importing this package registers the "gcp" provider, the way importing a
// database driver registers it. That is what lets a program choose GCP with
// `-target remote -provider gcp` while naming no cloud in its own code.
func init() { wings.RegisterProvider(&provider{}) }

type provider struct {
	project     string
	zone        string
	machineType string
	image       string
	diskGB      int64
	preemptible bool
	namePrefix  string
	user        string
}

func (p *provider) Name() string { return "gcp" }

// Flags are registered on the command line with a "gcp." prefix, so these
// become -gcp.project, -gcp.zone and so on.
func (p *provider) Flags(fs *flag.FlagSet) {
	fs.StringVar(&p.project, "project", "", "GCP project id (required for -provider gcp)")
	fs.StringVar(&p.zone, "zone", "", "GCP zone, e.g. europe-west1-b (required for -provider gcp)")
	fs.StringVar(&p.machineType, "machine-type", "", "machine type (default e2-standard-4)")
	fs.StringVar(&p.image, "image", "", "source image (default the latest Debian 12)")
	fs.Int64Var(&p.diskGB, "disk-gb", 0, "boot disk size in GB (default 20)")
	fs.BoolVar(&p.preemptible, "spot", false,
		"use preemptible Spot instances: much cheaper, and reclaimable mid-job")
	fs.StringVar(&p.namePrefix, "name-prefix", "", "prefix for instance names (default wings)")
	fs.StringVar(&p.user, "user", "", "Linux account to create and connect as (default wings)")
}

func (p *provider) New() (wings.Provisioner, error) {
	// Named individually rather than as one "incomplete configuration": the
	// whole value of this message is telling the user which flag to add.
	switch {
	case p.project == "" && p.zone == "":
		return nil, errors.New("-gcp.project and -gcp.zone are required")
	case p.project == "":
		return nil, errors.New("-gcp.project is required")
	case p.zone == "":
		return nil, errors.New("-gcp.zone is required")
	}

	return New(Config{
		Project:     p.project,
		Zone:        p.zone,
		MachineType: p.machineType,
		SourceImage: p.image,
		DiskSizeGB:  p.diskGB,
		Preemptible: p.preemptible,
		NamePrefix:  p.namePrefix,
		User:        p.user,
	}), nil
}
