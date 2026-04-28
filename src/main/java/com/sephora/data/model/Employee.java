package com.sephora.data.model;

public class Employee {

    private int id;
    private String nom;
    private String prenom;
    private int storeId;
    private String poste;
    private String departement;
    private String dateEmbauche;
    private String typeContrat;
    private double salaireBrut;

    public Employee(int id, String nom, String prenom, int storeId, String poste,
                    String departement, String dateEmbauche, String typeContrat, double salaireBrut) {
        this.id = id;
        this.nom = nom;
        this.prenom = prenom;
        this.storeId = storeId;
        this.poste = poste;
        this.departement = departement;
        this.dateEmbauche = dateEmbauche;
        this.typeContrat = typeContrat;
        this.salaireBrut = salaireBrut;
    }

    public int getId() {
        return id;
    }

    public String getNom() {
        return nom;
    }

    public String getPrenom() {
        return prenom;
    }

    public int getStoreId() {
        return storeId;
    }

    public String getPoste() {
        return poste;
    }

    public String getDepartement() {
        return departement;
    }

    public String getDateEmbauche() {
        return dateEmbauche;
    }

    public String getTypeContrat() {
        return typeContrat;
    }

    public double getSalaireBrut() {
        return salaireBrut;
    }

    @Override
    public String toString() {
        return "Employee{" +
                "id=" + id +
                ", nom='" + nom + '\'' +
                ", prenom='" + prenom + '\'' +
                ", storeId=" + storeId +
                ", poste='" + poste + '\'' +
                ", departement='" + departement + '\'' +
                ", dateEmbauche='" + dateEmbauche + '\'' +
                ", typeContrat='" + typeContrat + '\'' +
                ", salaireBrut=" + salaireBrut +
                '}';
    }
}