include build.properties

TAR=tar

source:
	rm -fr webgraph-$(version)
	ant clean
	ln -s . webgraph-$(version)
	$(TAR) chvf webgraph-$(version)-src.tar --owner=0 --group=0 \
		webgraph-$(version)/README.md \
		webgraph-$(version)/CHANGES \
		webgraph-$(version)/COPYING.LESSER \
		webgraph-$(version)/LICENSE-2.0.txt \
		webgraph-$(version)/build.xml \
		webgraph-$(version)/ivy.xml \
		webgraph-$(version)/webgraph.bnd \
		webgraph-$(version)/pom-model.xml \
		webgraph-$(version)/build.properties \
		webgraph-$(version)/src/overview.html \
		webgraph-$(version)/src/it/unimi/dsi/webgraph/*.{java,html} \
		webgraph-$(version)/src/it/unimi/dsi/webgraph/labelling/*.{java,html} \
		webgraph-$(version)/src/it/unimi/dsi/webgraph/algo/*.{java,html} \
		webgraph-$(version)/src/it/unimi/dsi/webgraph/jung/*.java \
		webgraph-$(version)/src/it/unimi/dsi/webgraph/test/SpeedTest.java \
		webgraph-$(version)/src/it/unimi/dsi/webgraph/examples/*.{java,html} \
		$$(find webgraph-$(version)/test/it/unimi/dsi/webgraph -iname \*.java | grep -v /scratch/ | grep -v /tool/) \
		$$(find webgraph-$(version)/slow/it/unimi/dsi/webgraph -iname \*.java) \
		webgraph-$(version)/slow/it/unimi/dsi/webgraph/cnr-2000*
	gzip -f webgraph-$(version)-src.tar
	rm webgraph-$(version)

binary:
	rm -fr webgraph-$(version)
	$(TAR) zxvf webgraph-$(version)-src.tar.gz
	(cd webgraph-$(version) && unset CLASSPATH && unset LOCAL_IVY_SETTINGS && ant ivy-clean ivy-setupjars && ant junit && ant clean && ant jar javadoc)
	$(TAR) zcvf webgraph-$(version)-bin.tar.gz --owner=0 --group=0 \
		webgraph-$(version)/README.md \
		webgraph-$(version)/CHANGES \
		webgraph-$(version)/COPYING.LESSER \
		webgraph-$(version)/LICENSE-2.0.txt \
		webgraph-$(version)/webgraph-$(version).jar \
		webgraph-$(version)/docs
	$(TAR) zcvf webgraph-$(version)-deps.tar.gz --owner=0 --group=0 --transform='s|.*/||' $$(find webgraph-$(version)/jars/runtime -iname \*.jar -exec readlink {} \;) 

stage:
	rm -fr webgraph-$(version)
	$(TAR) zxvf webgraph-$(version)-src.tar.gz
	cp -fr bnd webgraph-$(version)
	(cd webgraph-$(version) && unset CLASSPATH && unset LOCAL_IVY_SETTINGS && ant ivy-clean ivy-setupjars && ant stage)
