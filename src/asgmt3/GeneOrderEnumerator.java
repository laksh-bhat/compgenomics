package asgmt3;

import fileutils.FileUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * User: lbhat@damsl
 * Date: 10/6/13
 * Time: 1:01 PM
 */
public class GeneOrderEnumerator<T> {
    public static void main(String[] args) {
        List<String> input = FileUtils.getStringsFromStdIn();
        int inputNo = Integer.valueOf(input.get(0));
        List<Integer> permutation = new ArrayList<Integer>();
        for (int i = 1; i <= inputNo ; i++)
            permutation.add(i);
        GeneOrderEnumerator<Integer> geneOrderEnumerator = new GeneOrderEnumerator<Integer>();
        Collection<List<Integer>> permutations = geneOrderEnumerator.permute(permutation);
        System.out.println(permutations.size());
        for (List<Integer> perm : permutations){
            for (int i : perm)
                System.out.print(i + " ");
            System.out.println();
        }
    }

    public Collection<List<T>> permute(Collection<T> input) {
        Collection<List<T>> output = new ArrayList<List<T>>();
        if (input.isEmpty()) {
            output.add(new ArrayList<T>());
            return output;
        }
        List<T> list = new ArrayList<T>(input);
        T head = list.get(0);
        List<T> rest = list.subList(1, list.size());
        for (List<T> permutations : permute(rest)) {
            List<List<T>> subLists = new ArrayList<List<T>>();
            for (int i = 0; i <= permutations.size(); i++) {
                List<T> subList = new ArrayList<T>();
                subList.addAll(permutations);
                subList.add(i, head);
                subLists.add(subList);
            }
            output.addAll(subLists);
        }
        return output;
    }
}
