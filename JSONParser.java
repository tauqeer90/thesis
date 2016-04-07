import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.xml.bind.DatatypeConverter;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

/**
 * Class for parsing Compute Node JSON fetched over REST API.
 * @author Syed Tauqeer Hasan <syed.hasan@uni-ulm.de>
 *
 */
@SuppressWarnings("rawtypes")
public class JSONParser {

	JSONObject json = new JSONObject();

	//Class Constructor.
	public JSONParser(JSONObject jsonObject){
		//Initializing the JSON object.
		json = jsonObject;
	}

	/**
	 * Returns a list of Column values, parsed from the json file.
	 * @param entity whose values are to be extracted from JSON.
	 * @return the list of values of the specified entity.
	 */
	private List<String> getAllValuesFromJson(JSONEntities entity){
		// Array List which will contain all the values of the matrix.
		List<String> matrixValues = new ArrayList<String>();

		try {

			// Getting the Json objects of all the Rows.
			JSONArray jsonRows = json.getJSONArray("Row");

			// Converting the JSONArray into Json array to be used with GSON lib.
			JsonElement jsonElementAllRows = new JsonParser().parse(jsonRows.toString());
			// Converting the JsonElement into JsonArry of GSON lib. Array of JSON lib cannot be iterated.
			JsonArray myArray = jsonElementAllRows.getAsJsonArray();

			for (JsonElement elem : myArray)
			{
				// Access the element as a JsonObject
				JsonObject jsonObj = elem.getAsJsonObject();

				// Get the "Cell" element of the object which will return a JsonArray
				JsonArray tsPrimitive = jsonObj.getAsJsonArray("Cell");

				//Iterating Through the JsonArray returned by each Cell.
				for (JsonElement cellElem : tsPrimitive){
					// Access the element as a JsonObject
					JsonObject ab = cellElem.getAsJsonObject();

					//Get the matrix value.
					JsonPrimitive matrixValue = ab.getAsJsonPrimitive(entity.toString());
					//Decoding Base64 value
					String decodedValue = new String(DatatypeConverter.parseBase64Binary(matrixValue.toString()));
					// Converting the primitive as String and storing in the list.
					matrixValues.add(decodedValue);
				}
			}

		} catch (JSONException e) {
			System.out.println("JSONException: " + e);
		}

		return matrixValues;
	}

	/**
	 * Returns a HashMap with all values from JSON mapped with their matrices names.
	 * @return Mapped Hashmap
	 */
	private Multimap<String, String> mapMatricesWithValues(){
		// Array List which will contain all the values of particular entity from JSON file.
		List<String> columnsList = new ArrayList<String>();
		List<String> valuesList = new ArrayList<String>();
		// Parsing JSON Object.
		columnsList = getAllValuesFromJson(JSONEntities.column);
		valuesList = getAllValuesFromJson(JSONEntities.$); // $ represent the matrices names.

		// Mapping all Keys (column names) with Values.
		// Using Multimap, otherwise one key cannot have more than one value
		Multimap<String, String> mappedMatrices = ArrayListMultimap.create();

		for(int i=0; i<valuesList.size(); i++) {
			mappedMatrices.put(columnsList.get(i), valuesList.get(i));
		}

		//Returning Mapped Values.
		return mappedMatrices;
	}

	/**
	 * Returns all values of a particular matrix name present in json.
	 * @param matrixName whose values is to be extraced
	 * @return List of all values.
	 */
	public List<String> getAllMatrixValues(String matrixName){
		System.out.println("Parsing JSON values....");
		// List of matrix values to be read from json.
		List<String> matrixValues = new ArrayList<String>();
		// Multimap with values mapped with the matrix names.
		Multimap<String, String> mappedMatrices= mapMatricesWithValues();

		// Set of Matrix names.
		Set<String> matrixNames = mappedMatrices.keySet();
		// iterate through the key set and display key and values
		for (String name : matrixNames) {
			if( name.equals(matrixName)) {
				// Extracting values of the particular matrix.
				matrixValues.addAll(mappedMatrices.get(name));
			}
		}
		// return the list of values
		return matrixValues;
	}

	/**
	 * Returns a Set of all the matrices names present in the JSON file.
	 * @return Set of matrices names.
	 */
	public Set<String> getAllMatricesNames(){
		System.out.println("Parsing JSON values....");
		// Multimap with values mapped with the matrix names.
		Multimap<String, String> mappedMatrices= mapMatricesWithValues();
		// Set of Matrix names.
		Set<String> matrixNames = mappedMatrices.keySet();
		return matrixNames;
	}
}
