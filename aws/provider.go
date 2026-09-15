package aws

import (
	"errors"
	"flag"

	"github.com/ligustah/wings"
)

// Importing this package registers the "aws" provider, so a program can select
// it with `-target remote -provider aws` while naming no cloud in its own code.
func init() { wings.RegisterProvider(&provider{}) }

type provider struct {
	region       string
	instanceType string
	ami          string
	diskGB       int64
	subnet       string
	spot         bool
	namePrefix   string
	user         string
}

func (p *provider) Name() string { return "aws" }

// Flags registers -aws.region and the rest under an "aws." prefix.
func (p *provider) Flags(fs *flag.FlagSet) {
	fs.StringVar(&p.region, "region", "", "AWS region, e.g. eu-west-1 (required for -provider aws)")
	fs.StringVar(&p.instanceType, "instance-type", "", "instance type (default m5.xlarge)")
	fs.StringVar(&p.ami, "ami", "", "AMI id (default the latest Amazon Linux 2023 for the region)")
	fs.Int64Var(&p.diskGB, "disk-gb", 0, "root volume size in GB (default 500)")
	fs.StringVar(&p.subnet, "subnet", "", "subnet id (default a subnet of the region's default VPC)")
	fs.BoolVar(&p.spot, "spot", true,
		"use Spot instances: much cheaper, reclaimable mid-job, and the default; "+
			"pass -aws.spot=false for on-demand")
	fs.StringVar(&p.namePrefix, "name-prefix", "", "prefix for the instance Name tag (default wings)")
	fs.StringVar(&p.user, "user", "", "Linux account to create and connect as (default wings)")
}

func (p *provider) New() (wings.Provisioner, error) {
	if p.region == "" {
		return nil, errors.New("-aws.region is required")
	}
	return New(Config{
		Region:       p.region,
		InstanceType: p.instanceType,
		AMI:          p.ami,
		DiskSizeGB:   int32(p.diskGB),
		SubnetID:     p.subnet,
		Spot:         p.spot,
		NamePrefix:   p.namePrefix,
		User:         p.user,
	}), nil
}
