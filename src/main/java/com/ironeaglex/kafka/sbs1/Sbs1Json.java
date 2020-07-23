/**
 * sbs1_kafka_connector
 * Copyright (C) 2020  Iron EagleX
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.ironeaglex.kafka.sbs1;

public class Sbs1Json {

	private static final String SBS1_MSG_JSON_TEMPLATE =
		"{\"MT\":\"%1$s\"," +
		"\"TT\":\"%2$s\"," +
		"\"SID\":\"%3$s\"," +
		"\"AID\":\"%4$s\"," +
		"\"HEX\":\"%5$s\"," +
		"\"FID\":\"%6$s\"," +
		"\"DMG\":\"%7$s\"," +
		"\"TMG\":\"%8$s\"," +
		"\"DML\":\"%9$s\"," +
		"\"TML\":\"%10$s\",";

	private static final String SBS1_OTH_JSON_TEMPLATE =
		"{\"MT\":\"%1$s\"," +
		"\"SID\":\"%3$s\"," +
		"\"AID\":\"%4$s\"," +
		"\"HEX\":\"%5$s\"," +
		"\"FID\":\"%6$s\"," +
		"\"DMG\":\"%7$s\"," +
		"\"TMG\":\"%8$s\"," +
		"\"DML\":\"%9$s\"," +
		"\"TML\":\"%10$s\"";

	// private static final String SBS1_ALL_JSON_TEMPLATE =
	// 	SBS1_MSG_JSON_TEMPLATE +
	// 	"\"CS\":\"%11$s\"," +
	// 	"\"ALT\":\"%12$s\"," +
	// 	"\"GS\":\"%13$s\"," +
	// 	"\"TRK\":\"%14$s\"," +
	// 	"\"LAT\":\"%15$s\"," +
	// 	"\"LNG\":\"%16$s\"," +
	// 	"\"VR\":\"%17$s\"," +
	// 	"\"SQ\":\"%18$s\"," +
	// 	"\"ALRT\":\"%19$s\"," +
	// 	"\"EMER\":\"%20$s\"," +
	// 	"\"SPI\":\"%21$s\"," +
	// 	"\"GND\":\"%22$s\"}";

	private static final String SBS1_MSG1_JSON_TEMPLATE =
		SBS1_MSG_JSON_TEMPLATE +
		"\"CS\":\"%11$s\"}";

	private static final String SBS1_MSG2_JSON_TEMPLATE =
		SBS1_MSG_JSON_TEMPLATE +
		"\"ALT\":\"%12$s\"," +
		"\"GS\":\"%13$s\"," +
		"\"TRK\":\"%14$s\"," +
		"\"LAT\":\"%15$s\"," +
		"\"LNG\":\"%16$s\"," +
		"\"GND\":\"%22$s\"}";

	private static final String SBS1_MSG3_JSON_TEMPLATE =
		SBS1_MSG_JSON_TEMPLATE +
		"\"ALT\":\"%12$s\"," +
		"\"LAT\":\"%15$s\"," +
		"\"LNG\":\"%16$s\"," +
		"\"ALRT\":\"%19$s\"," +
		"\"EMER\":\"%20$s\"," +
		"\"SPI\":\"%21$s\"," +
		"\"GND\":\"%22$s\"}";

	private static final String SBS1_MSG4_JSON_TEMPLATE =
		SBS1_MSG_JSON_TEMPLATE +
		"\"GS\":\"%13$s\"," +
		"\"TRK\":\"%14$s\"," +
		"\"VR\":\"%17$s\"}";

	private static final String SBS1_MSG5_JSON_TEMPLATE =
		SBS1_MSG_JSON_TEMPLATE +
		"\"ALT\":\"%12$s\"," +
		"\"ALRT\":\"%19$s\"," +
		"\"SPI\":\"%21$s\"," +
		"\"GND\":\"%22$s\"}";

	private static final String SBS1_MSG6_JSON_TEMPLATE =
		SBS1_MSG_JSON_TEMPLATE +
		"\"ALT\":\"%12$s\"," +
		"\"SQ\":\"%18$s\"," +
		"\"ALRT\":\"%19$s\"," +
		"\"EMER\":\"%20$s\"," +
		"\"SPI\":\"%21$s\"," +
		"\"GND\":\"%22$s\"}";

	private static final String SBS1_MSG7_JSON_TEMPLATE =
		SBS1_MSG_JSON_TEMPLATE +
		"\"ALT\":\"%12$s\"," +
		"\"GND\":\"%22$s\"}";

	private static final String SBS1_MSG8_JSON_TEMPLATE =
		SBS1_MSG_JSON_TEMPLATE +
		"\"GND\":\"%22$s\"}";

	private static final String SBS1_SEL_JSON_TEMPLATE =
		SBS1_OTH_JSON_TEMPLATE + "," +
		"\"CS\":\"%11$s\"}";

	private static final String SBS1_ID_JSON_TEMPLATE =
		SBS1_OTH_JSON_TEMPLATE + "," +
		"\"CS\":\"%11$s\"}";

	private static final String SBS1_AIR_JSON_TEMPLATE =
		SBS1_OTH_JSON_TEMPLATE + "}";

	private static final String SBS1_STA_JSON_TEMPLATE =
		SBS1_OTH_JSON_TEMPLATE + "," +
		"\"CS\":\"%11$s\"}";

	private static final String SBS1_CLK_JSON_TEMPLATE =
		"{\"MT\":\"%1$s\"," +
		"\"SID\":\"%3$s\"," +
		"\"AID\":\"%4$s\"," +
		"\"FID\":\"%6$s\"," +
		"\"DMG\":\"%7$s\"," +
		"\"TMG\":\"%8$s\"," +
		"\"DML\":\"%9$s\"," +
		"\"TML\":\"%10$s\"}";

	public static String sbs1ToJson(String msg) {
		if ( null == msg || msg.isEmpty() ) {
			return null;
		}

		// Get rid of any CRLF characters on the end of the string
		msg = msg.trim();

		// Ensure there are 22 fields for the template
		// Object[] parts = new Object[22];
		// Arrays.fill(parts, "");
		// Object[] temp = msg.split(",");
		// System.arraycopy(temp, 0, parts, 0, temp.length);
		Object[] parts = msg.split(",");

		// Build the payload from the template
		String json = null;
		switch (parts[0].toString()) {
		case "MSG":
			switch (parts[1].toString()) {
				case "1": json = String.format(SBS1_MSG1_JSON_TEMPLATE, parts); break;
				case "2": json = String.format(SBS1_MSG2_JSON_TEMPLATE, parts); break;
				case "3": json = String.format(SBS1_MSG3_JSON_TEMPLATE, parts); break;
				case "4": json = String.format(SBS1_MSG4_JSON_TEMPLATE, parts); break;
				case "5": json = String.format(SBS1_MSG5_JSON_TEMPLATE, parts); break;
				case "6": json = String.format(SBS1_MSG6_JSON_TEMPLATE, parts); break;
				case "7": json = String.format(SBS1_MSG7_JSON_TEMPLATE, parts); break;
				case "8": json = String.format(SBS1_MSG8_JSON_TEMPLATE, parts); break;
			}
			break;
		case "SEL": json = String.format(SBS1_SEL_JSON_TEMPLATE, parts); break;
		case "ID":  json = String.format(SBS1_ID_JSON_TEMPLATE, parts); break;
		case "AIR": json = String.format(SBS1_AIR_JSON_TEMPLATE, parts); break;
		case "STA": json = String.format(SBS1_STA_JSON_TEMPLATE, parts); break;
		case "CLK": json = String.format(SBS1_CLK_JSON_TEMPLATE, parts); break;
		}

		return json;
	}

}
