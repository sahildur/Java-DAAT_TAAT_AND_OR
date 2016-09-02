/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 *
 * @author Sahil
 */
public class DAATTAATPROJ {

    public static void main(String[] args) {
        try {

            String fileindex = args[0];
            //   String fileindex="term.idx";
            String fileoutlog = args[1];
            //     String fileoutlog="out.log";            
            int topK = Integer.parseInt(args[2]);
            // int topK=10;
            String queryfile = args[3];
        //    String queryfile="sample_input.txt";

            ArrayList<Integer> DocIDs = new ArrayList<Integer>();
            ArrayList<Integer> DocIDsnorepeat = new ArrayList<Integer>();

            
            /////****** File reading ******/////
            File file = new File(fileindex);
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            StringBuffer stringBuffer = new StringBuffer();
            String line;

            HashMap<String, TermHolder> hmap = new HashMap<String, TermHolder>();
            int count = 1;
            
            
            while ((line = bufferedReader.readLine()) != null) {

                String[] parts = line.replace("\\", "\\\\").split("\\\\");

                int docfreq = Integer.parseInt(parts[2].substring(1, parts[2].length()));
                String dict = parts[4].substring(2, parts[4].length() - 1);
                String[] indparts = dict.split(", ");
                LinkedList<GroupOfDocIDandfreq> PostingsListgeneral = new LinkedList<GroupOfDocIDandfreq>();

                for (String s : indparts) {
                    String[] docfreqparts = s.split("/");
                    int docfreqind = Integer.parseInt(docfreqparts[1]);
                    int docIdind = Integer.parseInt(docfreqparts[0]);
                    DocIDs.add(docIdind);

                    if (!DocIDsnorepeat.contains(docIdind)) {

                        DocIDsnorepeat.add(docIdind);

                    } else {

                    }

                    PostingsListgeneral.add(new GroupOfDocIDandfreq(docIdind, docfreqind));
                }
                LinkedList<GroupOfDocIDandfreq> PostingsListbyDocID = new LinkedList<GroupOfDocIDandfreq>();
                LinkedList<GroupOfDocIDandfreq> PostingsListbytermfreq = new LinkedList<GroupOfDocIDandfreq>();

                Collections.sort(PostingsListgeneral, new Sortbytermfreq());
                PostingsListbytermfreq = PostingsListgeneral;

                LinkedList<GroupOfDocIDandfreq> copy = (LinkedList<GroupOfDocIDandfreq>) PostingsListgeneral.clone();

                Collections.sort(copy, new SortbyDocID());
                PostingsListbyDocID = copy;
                TermHolder temTermHolderp = new TermHolder(PostingsListbyDocID, PostingsListbytermfreq, docfreq);

                hmap.put(parts[0], temTermHolderp);
                count++;
            }

            FileWriter writer = new FileWriter(fileoutlog);
            PrintWriter pw = new PrintWriter(writer);

            
            /////// ***** top K called*****///////
            topkresult(hmap, topK, pw);
            pw.println("");
            pw.println("");
            
            
            //////////************** query file reading *****///////
            String sample_inp = queryfile;
            File fileI = new File(sample_inp);
            FileReader fileReaderI = new FileReader(fileI);
            BufferedReader bufferedReaderI = new BufferedReader(fileReaderI);
            StringBuffer stringBufferI = new StringBuffer();
            String lineI;
            int countI = 0;

            while ((lineI = bufferedReaderI.readLine()) != null) {

                ArrayList<String> queryterms = new ArrayList<String>();
                ArrayList<String> querytermsgetpostingsall = new ArrayList<String>();
                String[] indpartsI = lineI.split(" ");
                for (String s : indpartsI) {
                    querytermsgetpostingsall.add(s);
                    if ((TermHolder) hmap.get(s) != null) {
                        queryterms.add(s);
                    }
                }

                
                
                //////***** get posting called ******//////
                getpostingslist(hmap, querytermsgetpostingsall, pw);

                countI++;
 
                
                //////***** preparing Arraylist for optimised query ***//////////////
                    ArrayList<Termfreqonly> TFonly = new ArrayList<Termfreqonly>();
                    for (String q : queryterms) {

                        TermHolder th = (TermHolder) hmap.get(q);
                        if (th != null) {
                            TFonly.add(new Termfreqonly(q, (Integer) th.docfreq));
                        }
                    }

                    Collections.sort(TFonly, new tfocomparator());
                    ArrayList<String> optimisedqueryterms = new ArrayList<String>();

                    for (Termfreqonly tfo : TFonly) {
                        optimisedqueryterms.add(tfo.tfokey);
                    }
                /////***///////            
                
                
                /// ***object to store comparisons and times of with normal query term order***/////
                Comparisons compare = new Comparisons();
                Comparisons compareOr = new Comparisons();
                
                termAtATimeQueryAnd(hmap, queryterms, pw, compare, 0, querytermsgetpostingsall);
                
                ////optimized AND plus print
                termAtATimeQueryAnd(hmap, optimisedqueryterms, pw, compare, 1, querytermsgetpostingsall);
                
                
                termAtATimeQueryOr(hmap, queryterms, pw, compareOr, 0, querytermsgetpostingsall);
                
                ////** optimized OR plus print in file
                termAtATimeQueryOr(hmap, optimisedqueryterms, pw, compareOr, 1, querytermsgetpostingsall);
                
                
                documentAtaTimeQueryAnd(hmap, queryterms, pw,querytermsgetpostingsall);
                documentAtaTimeQueryOr(hmap, queryterms, pw,querytermsgetpostingsall);
            }

            pw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    
    ////////*********/////////
    ////////term at a time query AND function/////////
    ////////*********/////////
    public static void termAtATimeQueryAnd(HashMap mp, ArrayList<String> QueryList, PrintWriter pw, Comparisons compare, int type, ArrayList<String> Original) {
        int comp = 0;
        long startTime = System.currentTimeMillis();
///*** ACCUMULATOR***//
        ArrayList<Integer> documents_list_original_in_terms_orderaccumulator = new ArrayList<Integer>();

        int termcount = 0;
        if (type != 0) {
            //pw.println("");
            pw.print("FUNCTION: termAtATimeQueryAnd ");
            for (String tt : QueryList) {
                pw.print(tt + ", ");
            }
            //pw.println("");
        }

        for (String e : QueryList) {

            TermHolder th = (TermHolder) mp.get(e);
        
            /// ***temporary accumulator
            ArrayList<Integer> documents_list_original_temporary = new ArrayList<Integer>();

            for (GroupOfDocIDandfreq termslistbyfreqsorted : th.termListbytermfreq) {
                int present = 0;
                int finddoc = termslistbyfreqsorted.docID;

                
                if (termcount == 0) {
                
                    documents_list_original_temporary.add(finddoc);
                } else {

                    for (Integer scandocID : documents_list_original_in_terms_orderaccumulator) {
                //////*** comparisons counter
                        comp++;
                        if (scandocID == finddoc) {

                            present = 1;
                            break;
                        }
                    }
                    if (present == 1) {
                        documents_list_original_temporary.add(finddoc);
                        present = 0;
                    }
                }

            }
            termcount++;
            documents_list_original_in_terms_orderaccumulator = documents_list_original_temporary;

        }
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        if (type == 0) {
            compare.norcomp = comp;

            compare.nortime = totalTime;
        } else {

            pw.println("");
            pw.println(documents_list_original_in_terms_orderaccumulator.size() + "documents are found");
            pw.println(compare.norcomp + " comparisons are made");

            pw.println(compare.nortime + " ms are used");
            pw.println(comp + " comparisons are made with optimization (optional bonus part)");
            pw.print("Result: ");
            Collections.sort(documents_list_original_in_terms_orderaccumulator);
            for (Integer s : documents_list_original_in_terms_orderaccumulator) {
                pw.print(s + ", ");

            }

            pw.println("");
            //pw.println("");
        }
    }

    
    
    ////////*********/////////
    ////////term at a time query OR function/////////
    ////////*********/////////
    public static void termAtATimeQueryOr(HashMap mp, ArrayList<String> QueryList, PrintWriter pw, Comparisons compareOr, int type, ArrayList<String> Original) {
        int comp = 0;
        long startTime = System.currentTimeMillis();
        
        
        ///*** ACCUMULATOR***//
        ArrayList<Integer> documents_list_original_in_terms_orderaccumulator = new ArrayList<Integer>();

        int termcount = 0;
        if (type != 0) {
            pw.println("");
            pw.print("FUNCTION: termAtATimeQueryOR ");
            for (String tt : QueryList) {
                pw.print(tt + ", ");
            }
            //pw.println("");
        }
        for (String e : QueryList) {

            TermHolder th = (TermHolder) mp.get(e);
            ArrayList<Integer> documents_list_original_temporary = new ArrayList<Integer>();

            for (GroupOfDocIDandfreq termslistbyfreqsorted : th.termListbytermfreq) {
                int present = 0;
                int finddoc = termslistbyfreqsorted.docID;

                
                if (termcount == 0) {
                    documents_list_original_in_terms_orderaccumulator.add(finddoc);
                } else {
                    for (Integer scandocID : documents_list_original_in_terms_orderaccumulator) {
                        /////*** comparisons count
                        comp++;
                        if (scandocID == finddoc) {
                            present = 1;
                            break;
                        }
                    }
                    if (present == 1) {
                        
                        present = 0;
                    } else {
                        documents_list_original_in_terms_orderaccumulator.add(finddoc);

                    }
                }

            }
            
            termcount++;
        }
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        if (type == 0) {
            compareOr.norcomp = comp;

            compareOr.nortime = totalTime;
        } else {

            pw.println("");
            pw.println(documents_list_original_in_terms_orderaccumulator.size() + "documents are found");
            pw.println(compareOr.norcomp + " comparisons are made");

            pw.println(compareOr.nortime + "ms are used");
            pw.println(comp + " comparisons are made with optimization (optional bonus part)");
            pw.print("Result: ");
            Collections.sort(documents_list_original_in_terms_orderaccumulator);
            for (Integer s : documents_list_original_in_terms_orderaccumulator) {
                pw.print(s + ", ");

            }

            pw.println("");
            //pw.println("");
        }

    }

    ////////*********/////////
    ////////Document at a time query OR function/////////
    ////////*********/////////
    public static void documentAtaTimeQueryOr(HashMap mp, ArrayList<String> QueryList, PrintWriter pw,ArrayList<String> Original) {
        int comp = 0;
        long startTime = System.currentTimeMillis();
        pw.println("");
        pw.print("FUNCTION: documentAtaTimeQueryOr ");
        for (String tt : QueryList) {
            pw.print(tt + ", ");
        }
        //pw.println("");
        ArrayList<Integer> documents_list_final = new ArrayList<Integer>();

        LinkedList<GroupOfDocIDandfreq> pointer[] = new LinkedList[QueryList.size()];

        /////***** iterators for parallel pointers
        ListIterator<GroupOfDocIDandfreq> iterators[] = new ListIterator[QueryList.size()];
        
        ////*** priority queue for minimum in pointer positions
        ArrayList<docanditeratorPQ> queue = new ArrayList<docanditeratorPQ>();

        int count = 0;
        for (String e : QueryList) {
            TermHolder th = (TermHolder) mp.get(e);
            ListIterator<GroupOfDocIDandfreq> listIterator = th.termListbydocID.listIterator();
            pointer[count] = th.termListbydocID;
            iterators[count] = listIterator;
            count++;
        }

        for (int z = 0; z < QueryList.size(); z++) {

            if (iterators[z].hasNext()) {
                int nextid = iterators[z].next().docID;

                ///** extra loop for comparison count when maintaining priority queue for pointers to all list
                for (docanditeratorPQ temp : queue) {
                    if (nextid > temp.doc) {
///*****comparison internal counter
                        comp++;
                    } else {
                        break;
                    }
                }

                queue.add(new docanditeratorPQ(nextid, z));
            }

        }

        Collections.sort(queue, new sortcomparatorPQ());

        while (queue.size() != 0) {
            docanditeratorPQ d = queue.get(0);
            queue.remove(d);

            int doctoadd = d.doc;
            
            ////*******comparisons counter 
            comp++;
            if (!documents_list_final.isEmpty() && doctoadd == documents_list_final.get(documents_list_final.size() - 1)) {
            } else {
                documents_list_final.add(doctoadd);
            }

            if (iterators[d.iter].hasNext()) {
                int curr = iterators[d.iter].next().docID;
                queue.add(new docanditeratorPQ(curr, d.iter));
                Collections.sort(queue, new sortcomparatorPQ());

            }
        }

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        pw.println("");
        pw.println(documents_list_final.size() + " documents are found");
        pw.println(comp + " comparisons are made");

        pw.println(totalTime + " ms are used");

        pw.print("Result: ");
        for (Integer s : documents_list_final) {
            pw.print(s + ", ");

        }

        pw.println("");
        pw.println("");

    }
    /* 
     public static void documentAtaTimeQueryOr_old(HashMap mp,ArrayList<String> QueryList,ArrayList<Integer> DocIDsnorepeat)
     {
     ArrayList<Integer> documents_list_final=new ArrayList<Integer>();
        
     for(Integer docIdsearch:DocIDsnorepeat){
     System.out.println("For Doc Id"+docIdsearch+"||");
     for(String e:QueryList){
     int present=0;
     System.out.println("Query e"+e+"||");
     TermHolder th=(TermHolder)mp.get(e);
     for(GroupOfDocIDandfreq termslistbydocIDsorted :th.termListbydocID)
     {
     int comparedoc=termslistbydocIDsorted.docID;
     if(docIdsearch==comparedoc)
     {
     documents_list_final.add(docIdsearch);
     present=1;
     break;
     }                   
     }
     if(present==1)
     {
     present=0;
     break;
     }
            
     }
     }
     for(Integer s:documents_list_final)
     {
     System.out.println(s);
     }
     }
     
    
     */
    /*Old 
     public static void documentAtaTimeQueryAnd_old(HashMap mp,ArrayList<String> QueryList,ArrayList<Integer> DocIDsnorepeat)
     {
     ArrayList<Integer> documents_list_final=new ArrayList<Integer>();
        
     for(Integer docIdsearch:DocIDsnorepeat){
     int present=1;
     System.out.println("For Doc Id"+docIdsearch+"||");
     for(String e:QueryList){
             
     System.out.println("Query e"+e+"||");
     TermHolder th=(TermHolder)mp.get(e);
     int incurrentterm=0;
     for(GroupOfDocIDandfreq termslistbydocIDsorted :th.termListbydocID)
     {
            
     int comparedoc=termslistbydocIDsorted.docID;
     if(docIdsearch==comparedoc)
     {
     incurrentterm=1;
     break;
     }
               
     }
     if(incurrentterm==0)
     {
     present=0;
     break;
     }
            
            
     }
     if(present==1)
     {
     documents_list_final.add(docIdsearch);
     }
     }
     for(Integer s:documents_list_final)
     {
     System.out.println(s);
     }
     }
     */

    ////// *************************///////
    ////// term at a time AND query ///////
    ////// ************************ ///////
    public static void documentAtaTimeQueryAnd(HashMap mp, ArrayList<String> QueryList, PrintWriter pw,ArrayList<String> Original) {
        int comp = 0;
        long startTime = System.currentTimeMillis();

        pw.println("");
        pw.print("FUNCTION: documentAtaTimeQueryAnd ");
        for (String tt : QueryList) {
            pw.print(tt + ", ");
        }
       // pw.println("");

        ArrayList<Integer> documents_list_final = new ArrayList<Integer>();

        LinkedList<GroupOfDocIDandfreq> pointer[] = new LinkedList[QueryList.size()];
////***iterators for paralled pointers
        ListIterator<GroupOfDocIDandfreq> iterators[] = new ListIterator[QueryList.size()];
        int count = 0;
        for (String e : QueryList) {
            TermHolder th = (TermHolder) mp.get(e);
            ListIterator<GroupOfDocIDandfreq> listIterator = th.termListbydocID.listIterator();
            pointer[count] = th.termListbydocID;
            iterators[count] = listIterator;
            count++;
        }

        Integer input[] = new Integer[QueryList.size()];
        int cc = 0;

        ListIterator<GroupOfDocIDandfreq> firstlist = iterators[0];

        while (firstlist.hasNext()) {
            int found = 1;
            int temptocheck = firstlist.next().docID;

            for (int z = 1; z < QueryList.size(); z++) {

                if (iterators[z].hasNext()) {
                    int insideposting = 0;

                    while (iterators[z].hasNext()) {
                        int curr = iterators[z].next().docID;
////**** comparisons counter****///////
                        comp++;
                        if (curr == temptocheck) {

                            insideposting = 1;
                            break;
                        } else if (curr >= temptocheck) {

                            iterators[z].previous();
                            break;
                        } else {

                        }

                    }
                    if (insideposting == 1) {
                    } else {

                        found = 0;
                    }

                } else {

                    found = 0;
                    break;
                }

            }
            if (found == 1) {

                
                documents_list_final.add(temptocheck);
            }
        }
         

        for (int y = 0; y < QueryList.size(); y++) {

        }

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        pw.println("");
        pw.println(documents_list_final.size() + "documents are found");
        pw.println(comp + " comparisons are made");

        pw.println(totalTime + " ms are used");

        pw.print("Result: ");
        for (Integer s : documents_list_final) {
            pw.print(s + ", ");

        }

        pw.println("");
        //pw.println("");

        for (Integer s : documents_list_final) {

        }
    }

    
    ////// *************************///////
    ////// GET POSTINGS list function ///////
    ////// ************************ ///////
    
    public static void getpostingslist(HashMap mp, ArrayList<String> QueryList, PrintWriter pw) {

        for (String e : QueryList) {

            pw.println("FUNCTION: getPostings " + e);

            TermHolder th = (TermHolder) mp.get(e);
            if (th == null) {

                pw.print("TERM NOT FOUND");
                pw.println("");
                pw.println("");
                continue;
            }

            int flag = 0;
            pw.print("Ordered by Doc IDs: ");

            for (GroupOfDocIDandfreq termslistbydocIDsorted : th.termListbydocID) {
                flag = 1;

                pw.print(termslistbydocIDsorted.docID + ", ");
            }
            pw.println("");
            

            pw.print("Ordered by TF: ");
            for (GroupOfDocIDandfreq termslistbyfreqsorted : th.termListbytermfreq) {
                flag = 1;

                pw.print(termslistbyfreqsorted.docID + ", ");

            }
            pw.println("");
            pw.println("");
            

        }
        
    }

    
    ////// *************************///////
    ////// Top K Result function///////
    ////// ************************ ///////
   
    public static void topkresult(HashMap mp, int topK, PrintWriter pw) {
        long startTime = System.currentTimeMillis();

        Set set = mp.entrySet();
        Iterator iterator = set.iterator();

        ArrayList<TermanddocFreq> tosorttopK = new ArrayList<TermanddocFreq>();
        while (iterator.hasNext()) {
            Map.Entry mentry = (Map.Entry) iterator.next();

            TermHolder t = (TermHolder) mentry.getValue();

            tosorttopK.add(new TermanddocFreq((String) mentry.getKey(), t.docfreq));

        }
        Collections.sort(tosorttopK, new sortcomparatortopk());

        int count = 0;
        pw.print("FUNCTION: getTopK" + " " + topK);
        pw.println("");
        pw.print("Result: ");
        for (TermanddocFreq e : tosorttopK) {
            if (count == topK) {
                break;
            }
            if (count == topK - 1) {
                pw.print(e.termkey);
            } else {

                pw.print(e.termkey + ", ");
            }
            count++;
        }

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
            //pw.println(totalTime);

    }

   /* public static void printMap(HashMap mp) {
        Set set = mp.entrySet();
        Iterator iterator = set.iterator();

        while (iterator.hasNext()) {
            Map.Entry mentry = (Map.Entry) iterator.next();

            TermHolder t = (TermHolder) mentry.getValue();

            for (GroupOfDocIDandfreq e : t.termListbydocID) {

            }

            for (GroupOfDocIDandfreq e : t.termListbytermfreq) {

            }

        }
    }*/
}


/////*** holder for doc and freq to be used in linked list

class GroupOfDocIDandfreq {

    int docID;
    int termfreq;

    public GroupOfDocIDandfreq(int a, int b) {
        docID = a;
        termfreq = b;
    }
}

//// term holder for value in hashmap
class TermHolder// implements Comparable<TermHolder>
{

    LinkedList<GroupOfDocIDandfreq> termListbydocID;
    LinkedList<GroupOfDocIDandfreq> termListbytermfreq;
    int docfreq;

    public TermHolder(LinkedList l, LinkedList t, int b) {
        termListbydocID = l;
        termListbytermfreq = t;
        docfreq = b;
    }
}


//// Linked list 1 comparator
class SortbyDocID implements Comparator<GroupOfDocIDandfreq> {

    public int compare(GroupOfDocIDandfreq e1, GroupOfDocIDandfreq e2) {

        return e1.docID - e2.docID;
    }
}

//// **** Linked list 2 comparator
class Sortbytermfreq implements Comparator<GroupOfDocIDandfreq> {

    public int compare(GroupOfDocIDandfreq e1, GroupOfDocIDandfreq e2) {

        return e2.termfreq - e1.termfreq;
    }
}


//// **** holder for query terms to be sorted for optimization order***////
class Termfreqonly// implements Comparable<TermHolder>
{

    String tfokey;
    int tfodocfreq;

    public Termfreqonly(String l, int t) {
        tfokey = l;
        tfodocfreq = t;

    }
}
//// optimsed query terms comparator ////
class tfocomparator implements Comparator<Termfreqonly> {

    public int compare(Termfreqonly e1, Termfreqonly e2) {

        return e1.tfodocfreq - e2.tfodocfreq;
    }
}


////**** top K holder ****/////
class TermanddocFreq// implements Comparable<TermHolder>
{

    String termkey;
    int termdocfreq;

    public TermanddocFreq(String l, int t) {
        termkey = l;
        termdocfreq = t;

    }
}
////*** top K sort ***//
class sortcomparatortopk implements Comparator<TermanddocFreq> {

    public int compare(TermanddocFreq e1, TermanddocFreq e2) {

        return e2.termdocfreq - e1.termdocfreq;
    }
}


///***holder for priority queue****///
class docanditeratorPQ {

    int doc;
    int iter;

    public docanditeratorPQ(int l, int t) {
        doc = l;
        iter = t;

    }

}
////**** priority queue for parallel pointers sort ***///

class sortcomparatorPQ implements Comparator<docanditeratorPQ> {

    public int compare(docanditeratorPQ e1, docanditeratorPQ e2) {

        return e1.doc - e2.doc;
    }
}

////****
////Store term at a time AND/OR comparisons and time as same function is used to check with optimised and write to a file
class Comparisons {

    int norcomp;
    int optcomp;
    long nortime;

    public Comparisons() {
        norcomp = 0;
        optcomp = 0;
        nortime = 0;
    }

}
