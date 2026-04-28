package com.sephora.data.model;

public class Store {
    private  int id;
    private String nom;
    private String adresse;
    private String pays;
    private String region;
    private int surface;
    private String dateOuverture;
    private String status;
    private int nbEmployes;

    public Store(int id, String nom, String adresse, String pays, String region, int surface, String dateOuverture, String status, int nbEmployes) {
        this.id = id;
        this.nom = nom;
        this.adresse = adresse;
        this.pays = pays;
        this.region = region;
        this.surface = surface;
        this.dateOuverture = dateOuverture;
        this.status = status;
        this.nbEmployes = nbEmployes;
    }

    public int getId() {
        return id;
    }

    public String getNom() {
        return nom;
    }

    public String getAdresse() {
        return adresse;
    }

    public String getPays() {
        return pays;
    }

    public String getRegion() {
        return region;
    }

    public int getSurface() {
        return surface;
    }

    public String getDateOuverture() {
        return dateOuverture;
    }

    public String getStatus() {
        return status;
    }

    public int getNbEmployes() {
        return nbEmployes;
    }

    @Override
    public String toString() {
        return "Store{" +
                "id=" + id +
                ", nom='" + nom + '\'' +
                ", adresse='" + adresse + '\'' +
                ", pays='" + pays + '\'' +
                ", region='" + region + '\'' +
                ", surface=" + surface +
                ", dateOuverture='" + dateOuverture + '\'' +
                ", status='" + status + '\'' +
                ", nbEmployes=" + nbEmployes +
                '}';
    }
}
