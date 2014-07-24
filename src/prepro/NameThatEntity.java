package prepro;


import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.util.*;
import java.util.*;

public class NameThatEntity {

    protected StanfordCoreNLP pipeline;
//    PreparationSettings prepSettings;
//    DisambiguationSettings disSettings;
//    Preparator p;

    public NameThatEntity() {
        Properties props;
        props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma, ner");
        this.pipeline = new StanfordCoreNLP(props);

//        prepSettings = new StanfordHybridPreparationSettings();
//        p = new Preparator();
//        disSettings = new CocktailPartyDisambiguationSettings();
    }

    public TwoMaps tagIt(List<String> cleanedSentences) {
        TwoMaps maps = new TwoMaps();
        CoreLabel cur_token = null;
        String prev_token = "";

        for (String text : cleanedSentences) {
//            Prepare the input for disambiguation. The Stanford NER will be run
//            PreparedInput input = p.prepare(text, prepSettings);
//            Disambiguator d = new Disambiguator(input, disSettings);
//            DisambiguationResults results = d.disambiguate();

            // Print the disambiguation results.
//            for (ResultMention rm : results.getResultMentions()) {
//                ResultEntity re = results.getBestEntity(rm);
//                System.out.println(rm.getMention() + " -> " + re);
//            }
            Annotation document = new Annotation(text);
            this.pipeline.annotate(document);

            List<CoreMap> sentences = document.get(SentencesAnnotation.class);
            for (CoreMap sentence : sentences) {
                for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
                    cur_token = token;
                    maps.posMap = posIt(cur_token, maps.posMap);
                    maps.nerMap = nerIt(cur_token, prev_token, maps.nerMap);
                    prev_token = token.get(NamedEntityTagAnnotation.class);
                }
            }
        }
        return removeBadStuff(maps);
    }

//    private void disambiguateIt(DisambiguationResults results) {
//        Set<KBIdentifiedEntity> entities = new HashSet<KBIdentifiedEntity>();
//        for (ResultMention rm : results.getResultMentions()) {
//            entities.add(results.getBestEntity(rm).getKbEntity());
//        }
//
//        Map<KBIdentifiedEntity, EntityMetaData> entitiesMetaData
//                = DataAccess.getEntitiesMetaData(entities);
//
//        for (ResultMention rm : results.getResultMentions()) {
//            KBIdentifiedEntity entity = results.getBestEntity(rm).getKbEntity();
//            EntityMetaData entityMetaData = entitiesMetaData.get(entity);
//
//            if (Entities.isOokbEntity(entity)) {
//                System.out.println("\t" + rm + "\t NO MATCHING ENTITY");
//            } else {
//                System.out.println("\t" + rm + "\t" + entityMetaData.getId() + "\t"
//                        + entity + "\t" + entityMetaData.getHumanReadableRepresentation()
//                        + "\t" + entityMetaData.getUrl());
//            }
//        }
//    }

    private Map<String, List<String>> posIt(CoreLabel cur_token, Map<String, List<String>> posMap) {
        if (posMap.containsKey(cur_token.get(PartOfSpeechAnnotation.class))) {//Key has the POS tag
            if (!posMap.get(cur_token.get(PartOfSpeechAnnotation.class)).contains(cur_token.originalText())) {//Value NOT contains the pos
                posMap.get(cur_token.get(PartOfSpeechAnnotation.class)).add(cur_token.originalText());//Adds it
            }
        } else {//Key has NOT the POS tag
            List<String> pos = new ArrayList<String>();
            pos.add(cur_token.originalText());
            posMap.put(cur_token.get(PartOfSpeechAnnotation.class), pos);
        }
        return posMap;
    }

    private Map<String, List<String>> nerIt(CoreLabel cur_token, String prev_token, Map<String, List<String>> nerMap) {
        if (nerMap.containsKey(cur_token.get(NamedEntityTagAnnotation.class))) {//Key has the name tag
             /*What is the current key is the same as the previus?
             Then they must be categorized not only under the same Key,
             but also in the same List<String> value*/
            if (prev_token.equals(cur_token.get(NamedEntityTagAnnotation.class))) {//Same as previus                       
                int last_index = nerMap.get(cur_token.get(NamedEntityTagAnnotation.class)).size();
                String last_word = nerMap.get(cur_token.get(NamedEntityTagAnnotation.class)).remove(last_index - 1) + " " + cur_token.originalText();
                nerMap.get(cur_token.get(NamedEntityTagAnnotation.class)).add(last_word);
            } else {//Not same as previus
                if (!nerMap.get(cur_token.get(NamedEntityTagAnnotation.class)).contains(cur_token.originalText())) {//Value NOT contains the named enity
                    nerMap.get(cur_token.get(NamedEntityTagAnnotation.class)).add(cur_token.originalText());//Adds it
                }
            }
        } else { //Key has NOT the name tag
            List<String> ner = new ArrayList<String>();
            ner.add(cur_token.originalText());
            nerMap.put(cur_token.get(NamedEntityTagAnnotation.class), ner);
        }
        return nerMap;
    }

    private TwoMaps removeBadStuff(TwoMaps maps) {
        if (maps.nerMap.containsKey("O")) {//Remove "O" tag from map
            maps.nerMap.remove("O");
        }
        for (Iterator<Map.Entry<String, List<String>>> it = maps.posMap.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, List<String>> entry = it.next();
            if ((!entry.getKey().equals("NN")) && (!entry.getKey().equals("NNS")) && (!entry.getKey().equals("NNP")) && (!entry.getKey().equals("NNPS"))) {
                it.remove();
            }
        }
        return maps;
    }
}
