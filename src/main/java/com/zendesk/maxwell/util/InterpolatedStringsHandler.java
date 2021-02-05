package com.zendesk.maxwell.util;

import com.zendesk.maxwell.row.RowIdentity;
import com.zendesk.maxwell.row.RowMap;

public class InterpolatedStringsHandler {
	private final String copyOfInputString;
	private final boolean isInterpolated;
	public InterpolatedStringsHandler(String inputString) {
		this.copyOfInputString = inputString;
		this.isInterpolated = inputString.contains("%{");
	}

	public String generateFromRowIdentity(RowIdentity pk){
		String table = pk.getTable();
		if ( table == null )
			table = "";

		if ( this.isInterpolated )
			return this.copyOfInputString
					.replaceAll("%\\{database\\}", pk.getDatabase())
					.replaceAll("%\\{table\\}", table);
		else
			return this.copyOfInputString;
	}

	public String generateFromRowMap(RowMap r){
		String table = r.getTable();
		if ( table == null )
			table = "";

		String type = r.getRowType();

		if ( type == null )
			type = "";

		if ( this.isInterpolated )
			return this.copyOfInputString
					.replaceAll("%\\{database\\}", r.getDatabase())
					.replaceAll("%\\{table\\}", table)
					.replaceAll("%\\{type\\}", type);
		else
			return this.copyOfInputString;
	}
}
