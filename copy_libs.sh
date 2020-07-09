#!/bin/bash

LIB_DIR=lib/
mkdir -p $LIB_DIR

cp $TSK_HOME/bindings/java/dist/*.jar $LIB_DIR
cp $AUTOPSY_HOME/build/cluster/modules/ext/*.jar $LIB_DIR
cp $AUTOPSY_HOME/netbeans-plat/8.2/platform/lib/*.jar $LIB_DIR
cp $AUTOPSY_HOME/netbeans-plat/8.2/platform/modules/org-openide-awt.jar $LIB_DIR
cp $AUTOPSY_HOME/netbeans-plat/8.2/platform/modules/org-openide-windows.jar $LIB_DIR
cp $AUTOPSY_HOME/netbeans-plat/8.2/platform/modules/org-openide-dialogs.jar $LIB_DIR
cp $AUTOPSY_HOME/build/public-package-jars/org-openide-filesystems.jar $LIB_DIR
cp $AUTOPSY_HOME/CoreLibs/release/modules/ext/*.jar $LIB_DIR
cp $AUTOPSY_HOME/Core/release/modules/ext/*.jar $LIB_DIR
cp $AUTOPSY_HOME/build/cluster/modules/org-sleuthkit-*.jar $LIB_DIR
cp $AUTOPSY_HOME/KeywordSearch/release/modules/ext/*.jar $LIB_DIR
cp $IMAGE2CLUSTER_HOME/build/cluster/modules/org-rand-image2cluster.jar $LIB_DIR
