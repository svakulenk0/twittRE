package prepro;


import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TwoMaps {
    Map<String, List<String>> nerMap;
    Map<String, List<String>> posMap;
    
    public TwoMaps(){
        nerMap = new HashMap<String, List<String>>();
        posMap = new HashMap<String, List<String>>();
    }
    
    public void fillBothMaps(Map<String, List<String>> nerMap, Map<String, List<String>> posMap){
        this.nerMap.putAll(nerMap);
        this.posMap.putAll(posMap);
    }
     
    public Map<String, List<String>> getNerMap(){
        return nerMap;
    }
    
    public Map<String, List<String>> getPosMap(){
        return posMap;
    }  
}
