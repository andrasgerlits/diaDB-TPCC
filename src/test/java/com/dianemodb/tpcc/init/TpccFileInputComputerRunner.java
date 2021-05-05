package com.dianemodb.tpcc.init;

import com.dianemodb.Topology;
import com.dianemodb.integration.file.FileInputComputerRunner;
import com.dianemodb.metaschema.DianemoApplication;
import com.dianemodb.tpcc.TpccRunner;

public class TpccFileInputComputerRunner extends FileInputComputerRunner {

	public static void main(String[] args) throws Exception {
		executeRunner(args, new TpccFileInputComputerRunner());
	}
	
	@Override
	protected DianemoApplication createApplication(Topology topology) {
		return TpccRunner.createApplication(topology);
	}	
}
